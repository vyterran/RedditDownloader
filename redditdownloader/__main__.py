#!/usr/bin/env python3

import argparse
import multiprocessing
import sys
import static.stringutil as su
import static.settings as settings
import static.console as console
import static.metadata as meta
from sources import DirectInputSource, DirectURLSource, DirectFileSource
from interfaces.terminal import TerminalUI
from interfaces.eelwrapper import WebUI
import tests.runner
import tests.mock  # Required import to properly bootstrap tests when compiled.
import sql
from tools import ffmpeg_download
import static.filesystem as fs
import re
import logging


parser = argparse.ArgumentParser(
	description="Tool for scanning Reddit and downloading media - Guide @ https://goo.gl/hgBxN4")
parser.add_argument("--settings", help="Path to custom Settings file.", type=str, metavar='', default=None)
parser.add_argument("--source", '-s',
					help="Run each configured Source only if its alias matches the given pattern. Can pass multiple patterns.",
					type=str, action='append', metavar='')
parser.add_argument("--category.setting", help="Override the given setting(s).", action="store_true")
parser.add_argument("--list_settings", help="Display a list of overridable settings.", action="store_true")
parser.add_argument("--version", '-v', help="Print the current version and exit.", action="store_true")
parser.add_argument("--authorize", '-a', help="Authorize RMD with Reddit oAuth.", action="store_true")
parser.add_argument("--run_tests", help="Run the given test directory, or * for all.", type=str, metavar='', default="")
parser.add_argument("--limit", help="For direct downloading of user/subreddit, set the limit here.", type=int, default=1000)
parser.add_argument("--skip_update", help="If set, avoid checking for updates automatically", action="store_true")
parser.add_argument("--import_csv", help="Import all comments/posts from an export CSV file.", type=str, metavar='', default=None)
parser.add_argument("--full_csv", help="If set, include a slower method as a fallback when loading a CSV.", action="store_true")
parser.add_argument("--docker", help="If set, activate 'Docker Mode'.", action="store_true")
args, unknown_args = parser.parse_known_args()


direct_sources = []

def run():
	logging.basicConfig(level=logging.WARN, format='%(levelname)-5.5s [%(name)s] %(message)s', datefmt='%H:%M:%S')
	su.print_color('green', "\r\n" +
		'====================================\r\n' +
		('   Reddit Media Downloader %s\r\n' % meta.current_version) +
		'====================================\r\n' +
		'    (By ShadowMoose @ Github)\r\n')
	if args.version:
		sys.exit(0)

	if args.run_tests:
		error_count = tests.runner.run_tests(test_subdir=args.run_tests)
		sys.exit(error_count)

	if args.list_settings:
		print('All valid overridable settings:')
		for _s in settings.get_all():
			if _s.public:
				print("%s.%s" % (_s.category, _s.name))
				print('\tDescription: %s' % _s.description)
				if not _s.opts:
					print('\tValid value: \n\t\tAny %s' % _s.type)
				else:
					print('\tValid values:')
					for o in _s.opts:
						print('\t\t"%s": %s' % o)
				print()
		sys.exit()

	settings_file = args.settings or fs.find_file('settings.json')
	_loaded = settings_file is not None# settings.load(settings_file)
	for ua in unknown_args:
		if '=' not in ua or '/comments/' in ua:
			if '/comments/' in ua:
				direct_sources.append(DirectURLSource(url=ua))
				continue
			elif 'r/' or 'u/' in ua:
				direct_sources.append(DirectInputSource(txt=ua, args={'limit': args.limit}))
				continue
			else:
				su.error("ERROR: Unkown argument: %s" % ua)
				sys.exit(1)
		k = ua.split('=')[0].strip('- ')
		v = ua.split('=', 2)[1].strip()
		try:
			settings.put(k, v, save_after=False)
		except KeyError:
			print('Unknown setting: %s' % k)
			sys.exit(50)

	if args.source:
		matched_sources = set()
		for s in args.source:
			for stt in settings.get_sources():
				if re.match(s, stt.get_alias()):
					matched_sources.add(stt)
		direct_sources.extend(matched_sources)

	if args.import_csv:
		direct_sources.append(DirectFileSource(file=args.import_csv, slow_fallback=args.full_csv))

	first_time_auth = False

	if not _loaded and not direct_sources and not args.docker:
		# First-time configuration.
		su.error('Could not find an existing settings file. A new one will be generated!')
		if not console.confirm('Would you like to start the WebUI to help set things up?', True):
			su.print_color('red', "If you don't open the webUI now, you'll need to edit the settings file yourself.")
			if console.confirm("Are you sure you'd like to edit settings without the UI (if 'yes', these prompts will not show again)?"):
				settings.put('interface.start_server', False, save_after=True)  # Creates a save.
				print('A settings file has been created for you, at "%s". Please customize it.' % settings_file)
				first_time_auth = True
			else:
				print('Please re-run RMD to configure again.')
				sys.exit(1)
		else:
			mode = console.prompt_list('How would you like to open the UI?',
									   settings.get('interface.browser', full_obj=True).opts)
			settings.put('interface.browser', mode, save_after=False)
			settings.put('interface.start_server', True)

	if args.docker:
		print('Running in "Docker" mode. Assuming some default settings.')
		settings.put('interface.host', '0.0.0.0', save_after=False)
		settings.put('interface.browser', 'off', save_after=False)
		settings.put('interface.keep_open', True, save_after=False)
		settings.put('interface.start_server', True)

	if args.authorize or first_time_auth:  # In-console oAuth authentication flow
		from static import praw_wrapper
		from urllib.parse import urlparse, parse_qs
		url = praw_wrapper.get_reddit_token_url()
		su.print_color('green', '\nTo manually authorize your account, visit the below URL.')
		su.print_color('yellow', 'Once there, authorize RMD, then copy the URL it redirects you to.')
		su.print_color('yellow', 'NOTE: The redirect page will likely not load, and that is ok.')
		su.print_color('cyan', '\n%s\n' % url)
		token_url = console.col_input('Paste the URL you are redirected to here: ')
		if token_url.strip():
			qs = parse_qs(urlparse(token_url).query)
			if 'state' not in qs or 'code' not in qs:
				su.error('The url provided was not a valid reddit redirect. Please make sure you copied it right!')
			elif qs['state'][0].strip() != settings.get('auth.oauth_key').strip():
				su.error('Invalid reddit redirect state. Please restart and try again.')
			else:
				code = qs['code'][0]
				su.print_color('green', 'Got code. Authorizing account...')
				refresh = praw_wrapper.get_refresh_token(code)
				if refresh:
					settings.put('auth.refresh_token', refresh)
					usr = praw_wrapper.get_current_username()
					su.print_color('cyan', 'Authorized to view account: %s' % usr)
					su.print_color('green', 'Saved authorization token! Please restart RMD to begin downloading!')
				else:
					su.error('Failed to gain an account access token from Reddit with that code. Please try again.')
		sys.exit(0)

	if not ffmpeg_download.install_local():
		print("RMD was unable to locate (or download) a working FFmpeg binary.")
		print("For downloading and post-processing, this is a required tool.")
		print("Please Install FFmpeg manually, or download it from here: https://rmd.page.link/ffmpeg")
		sys.exit(15)

	# Initialize Database
	sql.init_from_settings()
	print('Using manifest file [%s].' % sql.get_file_location())

	if direct_sources:
		settings.disable_saving()
		settings.put('processing.retry_failed', False)
		for s in settings.get_sources():
			settings.remove_source(s, save_after=False)
		for d in direct_sources:
			settings.add_source(d, prevent_duplicate=False, save_after=False)

	if settings.get('interface.start_server') and not direct_sources:
		print("Starting WebUI...")
		ui = WebUI()
	else:
		ui = TerminalUI()
	ui.display()


if __name__ == '__main__':
	multiprocessing.freeze_support()
	run()

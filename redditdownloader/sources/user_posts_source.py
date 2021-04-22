from sources import source
import static.praw_wrapper as reddit
from static.settings import Setting
from sql import get_last_seen_posts

class UserPostsSource(source.Source):
	def __init__(self):
		super().__init__(source_type='user-posts-source', description="A User's Submission and/or Comment history.")
		
	def get_elements(self):
		check_last = self.data['check_last_seen_posts']
		if not check_last or check_last < 1:
			check_last = None
			last_seen = None
		else:
			last_seen = get_last_seen_posts(self.data['user'], check_last, self.data['check_last_seen_utc'])
		
		last_seen_i = None
		for re in reddit.user_posts(
				username=self.data['user'],
				find_submissions=self.data['scan_submissions'],
				find_comments=self.data['scan_comments'],
				find_limit=self.data['scan_limit'],
				deep_find_submissions=self.data['deep_scan_submissions'],
				deep_find_comments=self.data['deep_scan_comments']):
			if check_last is not None:
				res = self.is_new_post(re, last_seen)
				if type(res) is int:
					if last_seen_i is None:
						print("Reached start of last seen posts at: (%s/%s) [%s] %s %s"%(res, len(last_seen), re.strf_created_utc(), re.author, re.id), debug=True)
					last_seen_i = res
				elif res is False:
					print("Reached end of last seen posts at: (%s/%s) [%s] %s %s"%(last_seen_i, len(last_seen), re.strf_created_utc(), re.author, re.id), debug=True)
					break
			if self.check_filters(re):
				yield re

	def get_settings(self):
		yield Setting('user', '', etype='str', desc='Target username:')
		yield Setting('scan_comments', False, etype='bool', desc='Scan their comments?')
		yield Setting('scan_submissions', False, etype='bool', desc='Scan their submissions?')
		yield Setting('scan_limit', -1, etype='int', desc='Scan limit.')
		yield Setting('deep_scan_comments', False, etype='bool', desc='Scan their comments?')
		yield Setting('deep_scan_submissions', False, etype='bool', desc='Scan their submissions?')
		yield Setting('check_last_seen_posts', 0, etype='int', desc='Stop scanning after encountering saved posts.')
		yield Setting('check_last_seen_utc', 0, etype='int', desc='Look for encountered post before this utc seconds.')

	def get_config_summary(self):
		feeds = ""
		if self.data['scan_comments']:
			feeds += "Comments"
		if self.data['scan_submissions']:
			if len(feeds) > 0:
				feeds += " & "
			feeds += "Submissions"
		return "Scanning User (%s)'s %s." % (self.data['user'], feeds)
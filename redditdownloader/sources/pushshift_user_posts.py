from sources import source
from psaw import PushshiftAPI
from static.settings import Setting
from processing.wrappers.redditelement import RedditElement
from sql import get_last_seen_posts


class PushShiftUserSourceSource(source.Source):
	def __init__(self):
		super().__init__(source_type='pushshift-user-source', description="PushShift: The (possibly unlimited) posts made by a User.")

	def get_elements(self):		
		check_last = self.data['check_last_seen_posts']
		if not check_last or check_last < 1:
			check_last = None

		ps = PushshiftAPI()
		for user in self.data['users'].split(','):
			user = user.replace('/u/', '', 1).strip()
			_params = {'author': user}
			if self.data['limit']:
				_params['limit'] = self.data['limit']
			if self.data['scan_submissions']:
				if check_last is not None:
					last_seen = get_last_seen_posts(user, check_last, self.data['check_last_seen_utc'])
				else:
					last_seen = None
				
				last_seen_i = None
				for post in ps.search_submissions(**_params):
					p = RedditElement(post)
					if check_last is not None:
						res = self.is_new_post(p, last_seen)
						if type(res) is int:
							if last_seen_i is None:
								print("Reached start of last seen posts at: (%s/%s) [%s] %s %s"%(res, len(last_seen), p.strf_created_utc(), p.author, p.id), debug=True)
							last_seen_i = res
						elif res is False:
							print("Reached end of last seen posts at: (%s/%s) [%s] %s %s"%(last_seen_i, len(last_seen), p.strf_created_utc(), p.author, p.id), debug=True)
							break
					if self.check_filters(p):
						yield p
			if self.data['scan_comments']:
				for post in ps.search_comments(**_params):
					parents = list(ps.search_submissions(ids=post.link_id.replace('t3_', '', 1), limit=1))
					if not len(parents):
						print("PushShift Warning: Unable to locate parent Submission:", post.link_id)
						continue
					submission = parents[0]
					p = RedditElement(post, ext_submission_obj=submission)
					if self.check_filters(p):
						yield p

	def get_settings(self):
		yield Setting('users', '', etype='str', desc='Name of the desired user account(s), separated by commas:')
		yield Setting('limit', 1000, etype='int', desc='How many would you like to download from each? (0 for no limit):')
		yield Setting('scan_comments', False, etype='bool', desc='Scan their comments (very slow)?')
		yield Setting('scan_submissions', True, etype='bool', desc='Scan their submissions?')
		yield Setting('check_last_seen_posts', 0, etype='int', desc='Stop scanning after encountering saved posts.')
		yield Setting('check_last_seen_utc', 0, etype='int', desc='Look for encountered post before this utc seconds.')

	def get_config_summary(self):
		lim = self.data['limit']
		if lim > 0:
			lim = 'the first %s' % lim
		else:
			lim = 'all'
		return 'Downloading %s posts from user(s) "%s".' % (
			lim, self.data['users']
		)


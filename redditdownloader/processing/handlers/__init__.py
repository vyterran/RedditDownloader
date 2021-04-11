from urllib.parse import urlparse
from static import stringutil
import traceback


def sorted_list():
	""" A list of all available static Handlers, pre-sorted by order. """
	from processing.handlers import disabled_link, github, imgur, generic_newspaper, reddit_handler, ytdl, tumblr, direct_link, gfycat
	return sorted([
		disabled_link, generic_newspaper, github, imgur, reddit_handler, ytdl, tumblr, direct_link, gfycat
	], key=lambda x: x.order, reverse=False)


class HandlerTask:
	def __init__(self, url, file_obj):
		self.url = url
		self.file = file_obj


class HandlerResponse:
	def __init__(self, success, handler, rel_file=None, failure_reason=None, album_urls=()):
		self.rel_file = rel_file
		self.success = success
		self.handler = handler
		self.failure_reason = failure_reason
		self.album_urls = album_urls


def handle(handler_task, progress_obj):
	"""
	Pass the given HandlerTask into the handler list, and try to find one that can download the given file.
	"""
	res = urlparse(handler_task.url)
	for h in sorted_list():
		try:
			progress_obj.set_handler("%s on %s"%(h.tag, res.netloc))
			result = h.handle(task=handler_task, progress=progress_obj)
			if result:
				return result
		except Exception as ex:  # There are too many possible exceptions between all handlers to catch properly.
			stringutil.error('Handler Exception [%s] :: {%s} :: %s' % (h.tag, handler_task.url, ex))
			# We don't *really* care if a Handler fails, since there are plenty of reasons a download could.
			traceback.print_exc()
			pass
	return HandlerResponse(success=False, handler=None, failure_reason="No Handlers could process this URL.")

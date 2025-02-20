import requests
import mimetypes
from static import settings
from processing.handlers import HandlerResponse
import multiprocessing


allowed_mimetypes = ('image/', 'audio/', 'video/')


def _req_args():
	""" Settings all Requests should use. """
	return {
		'headers': {'User-Agent': settings.get('auth.user_agent')},
		'timeout': 10,
		'allow_redirects': True
	}


def open_request(url, stream=True):
	return requests.get(url, **_req_args(), stream=stream)


def is_media_url(url, return_status=False):
	ret = (None, None)
	try:
		r = requests.head(url, **_req_args())
		if not r or r.status_code != 200:
			ret = (None, r.status_code)
		else:
			ret = (_guess_media_mimetype(r), r.status_code)
	except Exception as ex:
		print(ex)
	if not return_status:
		ret = ret[0]
	return ret


def _guess_media_mimetype(req):
	if 'content-type' not in req.headers:
		return None
	content_type = req.headers['content-type']
	ext = mimetypes.guess_extension(content_type)

	if not ext or not any(content_type.startswith(t) for t in allowed_mimetypes):
		return None

	if content_type == 'image/jpeg':
		ext = '.jpg'
	ext = ext.strip('.')
	return ext


def download_binary(url, rel_file, prog, handler_id):
	"""
	Downloads the given URL into a binary file, updating the provided status as it goes.

	:param url: The URL to download
	:param rel_file: The RelFile to save to
	:param prog: The progress object to update.
	:param handler_id: The ID of the controlling Handler, for printout & Errors.
	:return: a HandlerResponse object, with the download outcome.
	"""
	# noinspection PyBroadException
	try:
		req = open_request(url, stream=True)
		if not req or req.status_code != 200:
			return HandlerResponse(success=False,
								   handler=handler_id,
								   failure_reason="Server Error: %s->%s" % (url, req.status_code if req is not None else None))
		size = req.headers.get('content-length')
		if size:
			size = int(size)
		downloaded_size = 0

		ext = _guess_media_mimetype(req)
		if not ext:
			return HandlerResponse(success=False, handler=handler_id, failure_reason="Unable to determine MIME Type.")
		rel_file.set_ext(ext)
		rel_file.mkdirs()
		prog.set_status("Downloading file...")
		prog.set_file(rel_file.relative())
		with open(rel_file.absolute(), 'wb') as f:
			for data in req.iter_content(chunk_size=1024*1024*4):
				downloaded_size += len(data)
				f.write(data)
				if size:
					prog.set_percent(round(100*(downloaded_size/size)))
		return HandlerResponse(success=True, rel_file=rel_file, handler=handler_id)
	except Exception as ex:
		print(ex)
		if rel_file.exists():
			rel_file.delete_file()
		return HandlerResponse(success=False, handler=handler_id, failure_reason="Error Downloading: %s" % ex)


def page_text(url, json=False):
	# noinspection PyBroadException
	try:
		req = open_request(url)
		if req.status_code != 200:
			return None
		if json:
			return req.json()
		return req.text
	except Exception:
		return None

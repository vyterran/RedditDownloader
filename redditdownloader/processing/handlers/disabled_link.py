from urllib.parse import urlparse
from processing.handlers import HandlerResponse

tag = 'disabled_link'
order = 0

disabled_list = """
youtu
youtube
amazon
twitter
instagram
onlyfans
chaturbate
""".strip().splitlines()

def handle(task, progress):  # !cover'
	res = urlparse(task.url)
	for domain in disabled_list:
		if domain.lower() in res.netloc.lower():
			return HandlerResponse(success=False, handler=tag, failure_reason="%s links are disabled."%domain)
	return False

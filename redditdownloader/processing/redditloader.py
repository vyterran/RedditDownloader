from logging import debug
import multiprocessing
import queue
from datetime import date, datetime
from static import settings
from processing import name_generator
from processing.wrappers import QueueReader, LoaderProgress
import sql



class RedditLoader(multiprocessing.Process):
	def __init__(self, sources, settings_json, db_lock):
		""" This is a daemon Loader class, which facilitates loading from multiple Sources
		 	and safely submitting their Posts to an internal queue.
		"""
		super().__init__()
		self.sources = sources
		self.settings = settings_json
		self._queue = multiprocessing.Queue(maxsize=2500)
		self._open_ack = set()
		self._ack_queue = multiprocessing.Queue()
		self._stop_event = multiprocessing.Event()  # This is a shared mp.Event, set when this reader should be done.
		self._stop_event.clear()
		self._reader = QueueReader(input_queue=self._queue, stop_event=self._stop_event)
		self._session = None
		self._lock = db_lock
		self.progress = LoaderProgress()
		self.daemon = True
		self.name = 'RedditElementLoader'

	def run(self):
		try:
			self.load()
		finally:
			print("run() is finished", debug=True)
			self._stop_event.set()

	def load(self):
		""" Threaded loading of elements. """
		settings.from_json(self.settings)
		sql.init_from_settings()
		self._session = sql.session()
		t_start = datetime.now() #vy
		print("Started loading.") #vy
		self.progress.set_scanning(True)

		retry_failed = settings.get('processing.retry_failed')

		# Query for all unhandled URLs, and submit them before scanning for new Posts.
		unfinished = self._session\
			.query(sql.URL)\
			.filter((sql.URL.processed == False) | \
				(retry_failed and sql.URL.failed and \
				 sql.not_(sql.URL.failure_reason.contains('404'))))\
			.all()
		print("Loading %s unfinished urls"%len(unfinished))
		self._push_url_list(unfinished)

		self._scan_sources()

		self.progress.set_scanning(False)
		# Wait for any remaining ACKS to come in, before closing the writing pipe.
		# ...Until the Downloaders have confirmed completion of everything, more album URLS may come in.
		while len(self._open_ack) > 0 and not self._stop_event.is_set():
			self._handle_acks(timeout=1.0, clear=True)
		print("Finished loading.") #vy
		print("Elapsed time: %s"%str(datetime.now()- t_start)) #vy
		sql.close()

	def _scan_sources(self):
		for source in self.sources:
			t_start = datetime.now()
			counter = 0
			print("Started scanning %s"%source.get_alias())
			try:
				self.progress.set_source(source.get_alias())
				for r in source.get_elements():
					if self._stop_event.is_set():
						print("_scan_sources says _stop_event.is_set", debug=True)
						return
					r.set_source(source)

					counter += 1
					# Create the SQL objects, then submit them to the queue.
					post = self._session.query(sql.Post).filter(sql.Post.reddit_id == r.id).first()
					with self._lock:
						if not post:
							post = sql.Post.convert_element_to_post(r)

						urls = self._create_element_urls(r, post)
						for u in urls:
							self._create_url_file(u, post=post)
						self._session.add(post)
						self._session.commit()
					self._push_url_list(urls)
				print("Finished scanning %s\n  %s posts in %s"%
						(source.get_alias(), counter, str(datetime.now()-t_start).rsplit('.',1)[0]))
			except ConnectionError as ce:
				print("Error while scanning %s\n  %s posts in %s"%
						(source.get_alias(), counter, (datetime.now()-t_start)))
				print(str(ce).upper())
			# TODO: Log failure.

	def _create_element_urls(self, reddit_element, post):
		"""
		Creates all the *new* URLS in the given RedditElement,
		then returns a list of the new URLs.
		"""
		urls = []
		for u in reddit_element.get_urls():
			if self._session.query(sql.URL.id).filter(sql.URL.address == u).first():
				# These URLS can be skipped, because they are top-level "non-album-file" urls.
				# Album URLs will be resubmitted submitted in a differet method.
				continue
			url = sql.URL.make_url(address=u, post=post, album_key=None, album_order=0)
			urls.append(url)
			self._session.add(url)
			post.urls.append(url)
		return urls

	def _create_album_urls(self, urls, post, album_key):
		""" Generates URL objects for Album URLs. """
		if not post:
			raise ValueError("The given Post does not exist - cannot generate Album URL: %s" % post)
		new_urls = []
		for idx, u in enumerate(urls):
			url = sql.URL.make_url(address=u, post=post, album_key=album_key, album_order=idx+1)
			new_urls.append(url)
			self._session.add(url)
			post.urls.append(url)
		return new_urls

	def _create_url_file(self, url, post, album_size=1):
		"""
		Builds the desired sql.File object for the given sql.URL Object.
		Automatically adds the File object to the URL.
		"""
		filename = name_generator.choose_file_name(url=url, post=post, session=self._session, album_size=album_size)
		file = sql.File(
			path=filename
		)
		self._session.add(file)
		url.file = file

	def count_remaining(self):
		""" Approximate the remaining elements in the queue. """
		return self._queue.qsize()

	def get_reader(self):
		return self._reader

	def get_ack_queue(self):
		return self._ack_queue

	def get_stop_event(self):
		return self._stop_event

	def _push_url_list(self, url_list, handle_acks=True):
		"""
		Submits the list of URLs to the Download Queue.
		:param url_list:
		:param handle_acks:
		:return:
		"""
		for u in url_list:
			self.progress.increment_found()
			while not self._stop_event.is_set():
				try:  # Keep trying to add this element to the queue, with a timeout to catch any stop triggers.
					self._queue.put(u.id, timeout=1)
					self._open_ack.add(u.id)  # Replace after testing.
					break
				except queue.Full:
					pass
		if handle_acks and len(self._open_ack) >= 100:
			timeout = max(1.0, min(60.0, 0.1*len(url_list)))
			self._handle_acks(timeout=timeout)  # passively process some ACKS in a non-blocking way to prevent queue bloat.

	def _handle_acks(self, timeout=0.1, clear=False):
		"""
		Process an Ack Packet in the queue, if there are any.
		If not, this method will return without blocking - unless `timeout` is set.
		"""
		if len(self._open_ack) == 0:
			return
		if not clear and len(self._open_ack) < 100:
			old_msg = self.progress.get_queue_size().split('.', 1)[1]
			self.progress.set_queue_size("%s acks remaining. %s"%(len(self._open_ack), old_msg))
			return
		self.progress.set_queue_size("%s acks remaining. Handling acks for %s sec ..."%(len(self._open_ack), timeout))
		start_time = datetime.now()
		count = 0
		try:
			while len(self._open_ack) > 0 and (datetime.now()-start_time).total_seconds() < timeout:
				packet = self._ack_queue.get(block=True, timeout=timeout)
				#print("handle_ack on packet %s"%packet, debug=True)
				url = self._session.query(sql.URL).filter(sql.URL.id == packet.url_id).first()
				#print("handle_ack on url %s"%url, debug=True)
				if packet.extra_urls:
					with self._lock:
						urls = self._create_album_urls(packet.extra_urls, url.post, url.album_id)
						for u in urls:
							self._create_url_file(u, post=url.post, album_size=len(urls))
						url.processed = True  # When the new URLs are committed, also prevent this URL from being reprocessed.
						self._session.commit()
					self._push_url_list(urls, handle_acks=False)
				else:
					with self._lock:
						url.processed = True
						self._session.commit()
				self._open_ack.remove(packet.url_id)
				count += 1
				msg = "%s acks remaining. Currently handled %s acks in %.3f of %.1f sec."\
						%(len(self._open_ack), count, (datetime.now()-start_time).total_seconds(), timeout)
				self.progress.set_queue_size(msg)
		except queue.Empty:
			pass
		msg = "%s acks remaining. Last handled %s acks in %.3f of %.1f sec."\
				%(len(self._open_ack), count, (datetime.now()-start_time).total_seconds(), timeout)
		#print(msg, debug=True)
		self.progress.set_queue_size(msg)
		return
		
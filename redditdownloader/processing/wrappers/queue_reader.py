import queue

from multiprocessing.util import debug


class QueueReader:
	"""
	The QueueReader is a **Pure Python, Serializable** class designed to simplify reading from queues safely.
	Instances of this class should be safe to share cross-Process.
	"""
	def __init__(self, input_queue, stop_event):
		"""
		Create a new QueueReader, a simplified class for standardized reading from Queues.
		:param input_queue: The Queue to read from
		:param stop_event: The multiprocessing.Event, which will tell this reader to stop reading.
		"""
		self._queue = input_queue
		self._stop_event = stop_event

	def next(self, hang=True):
		"""
		Gets the next Element in this Loader's Queue, if there is one. Hangs until one is available.
		:param hang: If False, throws a queue.Empty exception. Default True.
		:return: The next element, or None if loading is finished.
		"""
		while not self._stop_event.is_set():
			try:
				return self._queue.get(block=True, timeout=.1)
			except queue.Empty:
				if self._stop_event.is_set():
					#print("_stop_event is %s in QueueReader which has %s items"%(self._stop_event.is_set(), self._queue.qsize()), debug=True)
					return None
				if not hang:
					#print("_stop_event is %s in QueueReader which has %s items"%(self._stop_event.is_set(), self._queue.qsize()), debug=True)
					raise
		#print("_stop_event is %s in QueueReader which has %s items"%(self._stop_event.is_set(), self._queue.qsize()), debug=True)
		return None

	def __iter__(self):
		while True:
			#print("getting from QueueReader which has %s items"%self._queue.qsize(), debug=True)
			n = self.next(hang=True)
			if n is None:
				#print("no more items from QueueReader which has %s items"%self._queue.qsize(), debug=True)
				break
			#print("got item from QueueReader which has %s items"%self._queue.qsize(), debug=True)
			yield n

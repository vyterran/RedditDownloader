from logging import debug
from datetime import datetime
import multiprocessing
import traceback
import hashlib
from PIL import Image
import sql
from static import settings
from processing.wrappers import SanitizedRelFile, DownloaderProgress
from sql import File, URL, Hash
from sqlalchemy.orm import joinedload

# TODO: Once the stop_event is set, use the reader() class to submit filenames to the downloader threads, to split the Hashing job up.


class Deduplicator(multiprocessing.Process):
	def __init__(self, settings_json, stop_event, db_lock):
		"""
		Create a Hasher Process, which will be bound to the stop_event, performing post-processing on downloaded Files.
		"""
		super().__init__()
		self._settings = settings_json
		self._stop_event = stop_event
		self._lock = db_lock
		self.progress = DownloaderProgress()
		self.progress.clear(status="Starting up...")
		self._session = None
		self.daemon = True

	def run(self):
		""" Threaded loading of elements. """
		settings.from_json(self._settings)
		sql.init_from_settings()
		print("Starting up...", debug=True)
		try:
			self._session = sql.session()
			self.progress.clear(status="Starting up...")
			self.progress.set_running(True)
			self.dedup_ignore_ids = set()
			self.prune_counter = 0
			self.special_hashes = self._session.query(Hash).filter(Hash.id < 0).all()

			while not self._stop_event.is_set():
				#print("_stop_event is %s"%self._stop_event.is_set(), debug=True)
				completed = self._dedupe()
				if completed:
					self.progress.set_status("Completed %s files. Ready for new files..."%completed)
					self._stop_event.wait(1)
				else:
					self._stop_event.wait(10)
			print("_stop_event is %s"%self._stop_event.is_set(), debug=True)
			self._dedupe()  # Run one final pass after downloading stops.
			self.progress.clear(status="Finished.", running=False)
		except Exception as ex:
			print('Deduplication Process Error:', ex)
			self.progress.set_error(ex)
			self.progress.set_running(False)
			traceback.print_exc()
		finally:
			print("Finished process, _stop_event is %s"%self._stop_event.is_set(), debug=True)
			sql.close()

	def _dedupe(self):
		# unfinished = self._session\
		# 	.query(File) \
		# 	.options(joinedload(File.urls))\
		# 	.filter(File.hash == None)\
		# 	.filter(File.downloaded == True)\
		# 	.all()
		start_time = datetime.now()

		hashed = set(int(r.file_id) for r in self._session.query(Hash.file_id) \
															.filter(Hash.full_hash != None, Hash.file_id != None))
		downloaded = set(r.id for r in self._session.query(File).filter(File.downloaded == True))
		# get downloaded files without a hash
		search_ids = downloaded.difference(hashed).difference(self.dedup_ignore_ids)
		unfinished = self._session.query(File).filter(File.id.in_(search_ids)).all()

		unfinished = list(filter(lambda _f: not any(u.album_id for u in _f.urls), unfinished))  # Filter out albums.

		#print("Working on %s files total"%len(unfinished), debug=True)

		if not unfinished:
			return 0

		stats = {'unique':0, 'has_dup':0, 'special_hash':0, 'not_is_file':0, 'is_album':0}
		matches = []
		last_printed = ''
		for idx, f in enumerate(unfinished):
			self.progress.set_status("Deduplicating %s of %s files..."%(idx+1, len(unfinished)))
			#print("Working on  %s/%s files"%(idx, len(unfinished)), debug=True)
			path = SanitizedRelFile(base=settings.get("output.base_dir"), file_path=f.path)
			is_album = any(u.album_id for u in f.urls)
			if not path.is_file():
				stats['not_is_file'] += 1
				self.dedup_ignore_ids.add(f.id)
				continue
			if is_album:
				stats['is_album'] += 1
				self.dedup_ignore_ids.add(f.id)
				continue
			if self._stop_event.is_set():
				break
			new_hash = FileHasher.get_best_hash(path.absolute())
			# print('New hash for File:', f.id, '::', new_hash)
			for h in self.special_hashes:
				if new_hash == h.full_hash:
					print("Found special hash:", h, "::\n", f, debug=True)
					stats['special_hash'] += 1
					with self._lock:
						f.hash = Hash.make_hash(f, new_hash)
						self._session.query(URL).filter(URL.file_id == f.id).update({URL.file_id: h.file_id})
						file = SanitizedRelFile(base=settings.get("output.base_dir"), file_path=f.path)
						if file.is_file():
							file.delete_file()
						self._session.commit()
						break
			else: # not a special hash
				matches = self._find_matching_files(new_hash, ignore_id=f.id)
				if matches:
					if new_hash == last_printed:
						print("Found another duplicate:", new_hash, "::\n", f, debug=True)
					elif len(matches) > 6:
						printed = matches[3:] + ["... %s total matches ..."%len(matches)] + matches[:-3]
						print("Found duplicate files: ", new_hash,"::\n", '\n'.join(str(m) for m in [f]+printed), debug=True)
					else:
						print("Found duplicate files: ", new_hash,"::\n", '\n'.join(str(m) for m in [f]+matches), debug=True)
					stats['has_dup'] += 1
					last_printed = new_hash
				else:
					stats['unique'] += 1
				# print('\tActual matches:', matches)
				with self._lock:
					f.hash = Hash.make_hash(f, new_hash)
					#print("Updating hash: ", f.id, f.hash.file_id, f.hash, debug=True)
					if len(matches):
						#print("Found duplicate files: ", new_hash, "::", [(m.id, m.path) for m in matches])
						best, others = self._choose_best_file(matches + [f])
						# print('Chose best File:', best.id)
						for o in others:
							self._upgrade_file(new_file=best, old_file=o)
					self._session.commit()
				if matches:
					print("Completed %s of %s files..."%(idx+1, len(unfinished)), debug=True)
		dt = datetime.now() - start_time
		print("Completed all %s files in %s sec. Counts = %s"%(len(unfinished), str(dt), ', '.join('%s: %s'%(k,v) for k,v in stats.items() if v)), debug=True)
		# self.prune_counter += len(matches)
		# if self.prune_counter >= 100:
		# 	self.prune_counter = 0
			#self.progress.set_status("Pruning orphaned files...")
			#self._prune()
			#print("Finished pruning.", debug=True)
		return len(unfinished)

	def _find_matching_files(self, search_hash, ignore_id):
		sp = Hash.split_hash(search_hash)
		all_hashes = self._session \
			.query(File) \
			.join(Hash, File.hash) \
			.filter(
				(Hash.full_hash == search_hash) |
				(Hash.p1 == sp[0]) |
				(Hash.p2 == sp[1]) |
				(Hash.p3 == sp[2]) |
				(Hash.p4 == sp[3])
			).all()
		# print(sp)
		# print('Potential matches:', len(all_hashes), all_hashes)
		return list(filter(lambda f: self._check_hash_match(f, search_hash), all_hashes))

	def _check_hash_match(self, file, search_hash):
		""" Compare the given hash against the given SQL File.
			Returns invalid if the target File has albums, or is not fully processed.
		"""
		if not file.hash or any(u.album_id or not u.processed for u in file.urls):
			return False
		#if FileHasher.hamming_distance(search_hash, file.hash.full_hash) >= 4:
		if search_hash != file.hash.full_hash:
			return False
		return True

	def _choose_best_file(self, files):
		files = sorted(
			files,
			key=lambda f: SanitizedRelFile(base=settings.get("output.base_dir"), file_path=f.path).size(),
			reverse=True
		)
		return files[0], files[1:]

	def _upgrade_file(self, new_file, old_file):
		# print('Upgrading old file:', old_file.id, old_file.path, ' -> ', new_file.id, new_file.path)
		self._session.query(URL). \
			filter(URL.file_id == old_file.id). \
			update({URL.file_id: new_file.id})
		file = SanitizedRelFile(base=settings.get("output.base_dir"), file_path=old_file.path)
		if file.is_file():
			file.delete_file()

	def _prune(self):
		with self._lock:
			files_id = set(r.id for r in self._session.query(File))
			url_files_id = set(int(r.file_id) for r in self._session.query(URL))
			orphans = self._session.query(File).filter(File.id.in_(files_id.difference(url_files_id))).delete(synchronize_session='fetch')
			#orphans = self._session.query(File).filter(~File.urls.any()).delete(synchronize_session='fetch')
			self._session.commit()
			if orphans:
				print("Deleted orphan Files:", orphans, debug=True)


class FileHasher:
	@staticmethod
	def get_best_hash(filename):
		"""
		Attempts to hash the given file with the best possible hash (either a direct SHA1 or a Visual)
		:param filename: The path to hash.
		"""
		try:
			image = Image.open(filename)
			if FileHasher._is_animated(image):
				# Could dhash gifs to compare them, but that's a lot of memory for little likely gain.
				best_hash = FileHasher._sha_hash(filename)
			else:
				best_hash = FileHasher._dhash(image)
			image.close()
		except IOError:
			# Pillow can't load the file, so we have to assume it's not an image.
			best_hash = FileHasher._sha_hash(filename)
		return best_hash

	@staticmethod
	def _is_animated(image):
		"""
		Checks if the given Image object is an animated GIF
		"""
		# noinspection PyBroadException
		try:
			image.seek(1)
		except Exception:
			return False
		else:
			return True

	@staticmethod
	def _dhash(image, hash_size=8):
		"""
		Generates a Visual Difference Hash of the given Image Object.
		Credit to: https://github.com/JohannesBuchner/imagehash
		"""
		# Grayscale and shrink the image in one step.
		image = image.convert('L').resize(
			(hash_size + 1, hash_size),
			Image.ANTIALIAS,
		)
		# Compare adjacent pixels.
		difference = []
		for row in range(hash_size):
			for col in range(hash_size):
				pixel_left = image.getpixel((col, row))
				pixel_right = image.getpixel((col + 1, row))
				difference.append(pixel_left > pixel_right)
		# Convert the binary array to a hexadecimal string.
		decimal_value = 0
		hex_string = []
		for index, value in enumerate(difference):
			if value:
				decimal_value += 2**(index % 8)
			if (index % 8) == 7:
				hex_string.append(hex(decimal_value)[2:].rjust(2, '0'))
				decimal_value = 0
		return ''.join(hex_string)

	@staticmethod
	def _sha_hash(filename):
		try:
			with open(filename, 'rb', buffering=0) as f:
				h = hashlib.sha1()
				for b in iter(lambda: f.read(1024*1024), b''):
					h.update(b)
				return h.hexdigest()
		except IOError:
			return None

	@staticmethod
	def hamming_distance(s1, s2):
		"""Return the Hamming distance between equal-length sequences"""
		if len(s1) != len(s2):
			return 9999
		return sum(el1 != el2 for el1, el2 in zip(s1, s2))


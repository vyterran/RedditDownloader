import sys, os
import time
import logging
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, PatternMatchingEventHandler

class MyEventHandler(PatternMatchingEventHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.logger = logging.root

    def on_moved(self, event):
        super().on_moved(event)

        what = 'directory' if event.is_directory else 'file'
        self.logger.info("Moved %s: from %s to %s", what, event.src_path,
                         event.dest_path)

    def on_created(self, event):
        super().on_created(event)

        what = 'directory' if event.is_directory else 'file'
        self.logger.info("Created %s: %s", what, event.src_path)

    def on_deleted(self, event):
        super().on_deleted(event)

        what = 'directory' if event.is_directory else 'file'
        self.logger.info("Deleted %s: %s", what, event.src_path)

    def on_modified(self, event):
        super().on_modified(event)

        what = 'directory' if event.is_directory else 'file'
        self.logger.info("Modified %s: %s", what, event.src_path)

class MyDirEventHandler(PatternMatchingEventHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.logger = logging.root

    @staticmethod
    def last_dir(path):
        part = os.path.split(path)
        if '.' not in part[1]:
            return part[1]
        return os.path.split(part[0])[1]

    def on_moved(self, event):
        super().on_moved(event)

        what = 'directory' if event.is_directory else 'file'
        self.logger.info("Moved %s: from %s to %s", what, self.last_dir(event.src_path),
                          self.last_dir(event.dest_path))

    def on_created(self, event):
        super().on_created(event)

        what = 'directory' if event.is_directory else 'file'
        self.logger.info("Created %s: %s", what, self.last_dir(event.src_path))

    def on_deleted(self, event):
        super().on_deleted(event)

        what = 'directory' if event.is_directory else 'file'
        self.logger.info("Deleted %s: %s", what, self.last_dir(event.src_path))

    def on_modified(self, event):
        super().on_modified(event)

        what = 'directory' if event.is_directory else 'file'
        self.logger.info("Modified %s: %s", what, self.last_dir(event.src_path))

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(message)s',
                        datefmt='%H:%M:%S')
    path = sys.argv[1] if len(sys.argv) > 1 else '../users'
    
    event_handler = MyDirEventHandler(patterns=None,
                        ignore_patterns=['*.sqlite*'], ignore_directories=False)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    logging.info("observing changes in %s"%path)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("stopped via keyboard interrupt")
    finally:
        observer.stop()
        observer.join()
"""Operating-system journal ownership; automatically released on process death."""
from pathlib import Path
import os
from .protocol import ServiceError


class JournalOwner:
    def __init__(self, path):
        self.file = None
        if str(path) == ':memory:':
            return
        target = Path(path).resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(str(target) + '.owner', 'a+b')
        try:
            if os.name == 'nt':
                import msvcrt
                if self.file.tell() == 0:
                    self.file.write(b'0'); self.file.flush()
                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            self.file = None
            raise ServiceError('journal_in_use', 'journal already has an active owner', 409) from None

    def close(self):
        if self.file is not None:
            self.file.close()
            self.file = None

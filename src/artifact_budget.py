"""Bounded immutable artifact writes per physical job, not per logical worker."""
import threading
from raw_capture import CheckpointStorageError

MAX_UPLOADS=450  # GitHub limit 500 per job, reserve margin for reporting/retry


class ArtifactBudget:
    def __init__(self,storage,limit=MAX_UPLOADS):
        if type(limit) is not int or not 1<=limit<=MAX_UPLOADS:
            raise ValueError("Invalid physical artifact budget")
        self.storage=storage
        self.limit=limit
        self.used=0
        self.lock=threading.Lock()

    @property
    def exhausted(self):
        with self.lock:return self.used>=self.limit

    def __call__(self,op,name,folder):
        if op=="upload":
            with self.lock:
                if self.used>=self.limit:
                    raise CheckpointStorageError("Physical job artifact budget exhausted")
                self.used+=1
        try:
            return self.storage(op,name,folder)
        except CheckpointStorageError:
            raise
        except Exception as exc:
            raise CheckpointStorageError("Durable artifact operation failed: "+op) from exc

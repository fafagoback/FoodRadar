"""Thread-local transport reuse with fresh logical sessions and cookie jars."""
import threading
import requests

class TransportPool:
    def __init__(self):
        self.local = threading.local()
        self.lock = threading.Lock()
        self.adapters = []

    def session(self):
        adapter = getattr(self.local, "adapter", None)
        if adapter is None:
            adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=2, max_retries=0)
            self.local.adapter = adapter
            with self.lock:
                self.adapters.append(adapter)
        session = requests.Session()
        # Close the unused default adapter before replacing it.
        session.adapters["https://"].close()
        session.mount("https://", adapter)
        close = session.close
        def close_session():
            # The session owns all cookies and headers; only the transport
            # belongs to this worker thread until the worker pool shuts down.
            if session.adapters.get("https://") is adapter:
                session.adapters.pop("https://")
            close()
        session.close = close_session
        return session

    def close(self):
        for adapter in self.adapters:
            adapter.close()
        self.adapters.clear()

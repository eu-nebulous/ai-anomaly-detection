

from proton import Link as pLink

class Link:

    fqdn=False
    context=None

    def __init__(self, key, address, topic=False, fqdn=False):
        super().__init__()
        self.key = key
        self.address = address
        self.topic= topic
        self.fqdn= fqdn
        self._link = None

    def set(self, link:pLink):
        # The proton container creates a sender
        # so we just use composition instead of extension
        self._link = link

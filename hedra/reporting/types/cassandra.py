try:

    from cassandra.cluster import Cluster
    from cassandra.io.asyncioreactor import AsyncioConnection
    from cassandra.auth import PlainTextAuthProvider
    has_connector = True

except Exception:
    has_connector = False


class Cassandra:

    def __init__(self) -> None:
        self.session = None

    async def connect(self):
        cluster = Cluster(connection_class=AsyncioConnection)
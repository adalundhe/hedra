from .servers import JobServer


server = JobServer()
server.setup()
app = server.get_app()
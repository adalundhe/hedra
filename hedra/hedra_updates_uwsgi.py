from .servers import UpdateServer


server = UpdateServer()
server.setup()
app = server.get_app()
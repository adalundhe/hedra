from .command import Command


class Result:

    def __init__(self, command: Command, error: Exception=None) -> None:
        self.type = 'playwright'
        self.name = command.name
        self.error = error
        self.time = 0
        self.user = command.metadata.user
        self.tags = command.metadata.tags
        self.url = command.url.location
        self.headers = command.url.headers
        self.checks = command.checks
        self.method = command.command
        self._selector = command.page.selector
        self._x_coord = command.page.x_coordinate
        self._y_coord = command.page.y_coordinate
        self._frame = command.page.frame
        self.data = None
        self.hostname = None

    @property
    def path(self):
        if self._frame:
            return self._frame
        
        elif self._x_coord is not None and self._y_coord is not None:
            return f'{self._x_coord},{self._y_coord}'
        
        else:
            return self._selector
from email.policy import default
from typing import Any, Union
from urllib.parse import urlparse
from .command import Command
from .result import Result


class Context:
    values = {}
    last: Result = None

    def __getitem__(self, key: str):
        return self.values.get(key)

    def __setitem__(self, key: str, value: str):
        self.values[key] = value

    def update_command(self, command: Command):
        command.name = self.values.get('name', command.name)
        command.command = self.values.get('command', command.command)
        command.page.selector = self.values.get('selector', command.page.selector)
        command.page.attribute = self.values.get('attribute', command.page.attribute)
        command.page.x_coordinate = self.values.get('x_coordinate', command.page.x_coordinate)
        command.page.y_coordinate = self.values.get('y_coordinate', command.page.y_coordinate)
        command.page.frame = self.values.get('frame', command.page.frame)
        command.url.location = self.values.get('url', command.url.location)
        command.url.headers = self.values.get('headers', command.url.headers)
        command.input.key = self.values.get('key', command.input.key)
        command.input.text = self.values.get('text', command.input.text)
        command.input.function = self.values.get('function', command.input.function)
        command.input.args = self.values.get('args', command.input.args)
        command.input.filepath = self.values.get('filepath', command.input.filepath)
        command.input.file = self.values.get('file', command.input.file)
        command.options.event = self.values.get('event', command.options.event)
        command.options.is_checked = self.values.get('is_checked', command.options.is_checked)
        command.options.timeout = self.values.get('timeout', command.options.timeout)
        command.options.extra = self.values.get('extra', command.options.extra)
        command.options.switch_by = self.values.get('switch_by', command.options.switch_by)
        command.checks = self.values.get('checks', command.checks)
        command.metadata.tags = self.values.get('tags', command.metadata.tags)
        command.metadata.user = self.values.get('user', command.metadata.user)

        return command
from typing import Dict, List
from hedra.core.engines.types.common.metadata import Metadata


class Page:

    def __init__(self, selector: str=None, attribute: str=None, x_coordinate: int=0, y_coordinate: int=0, frame=0) -> None:
        self.selector = selector
        self.attribute = attribute
        self.x_coordinate = x_coordinate
        self.y_coordinate = y_coordinate
        self.frame = frame

class URL:

    def __init__(self, location: str=None, headers: Dict[str, str]={}) -> None:
        self.location = location
        self.headers = headers

class Input:

    def __init__(self, key=None, text=None, function=None, args=None, filepath=None, file=None) -> None:
        self.key = key
        self.text = text
        self.function = function
        self.args = args
        self.filepath = filepath
        self.file = file

class Options:

    def __init__(self, event=None, option=None, is_checked=False, timeout=10, extra={}, switch_by: str='url') -> None:
        self.event = event
        self.option = option
        self.is_checked = is_checked
        self.timeout = timeout
        self.extra = extra
        self.switch_by = switch_by

class Command:
    
    def __init__(self, name, command, page: Page=Page(), url: URL=URL(), input: Input=Input(), options: Options=Options(), user: str=None, tags: List[Dict[str, str]]=[], checks=None) -> None:
        self.name = name
        self.command = command
        self.page = page
        self.url = url
        self.input = input
        self.options = options
        self.checks = checks
        self.metadata = Metadata(user, tags)
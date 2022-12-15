from typing import Coroutine, Dict, List, Any
from hedra.core.engines.types.common.metadata import Metadata
from .hooks import Hooks


class Page:

    def __init__(self, selector: str=None, attribute: str=None, x_coordinate: int=0, y_coordinate: int=0, frame=0) -> None:
        self.selector = selector
        self.attribute = attribute
        self.x_coordinate = x_coordinate
        self.y_coordinate = y_coordinate
        self.frame = frame

    def to_serializable(self):
        return {
            'selector': self.selector,
            'attribute': self.attribute,
            'x_coordinage': self.x_coordinate,
            'y_coordinate': self.y_coordinate,
            'frame': self.frame
        }


class URL:

    def __init__(self, location: str=None, headers: Dict[str, str]={}) -> None:
        self.location = location
        self.headers = headers

    def to_serializable(self):
        return {
                'location': self.location,
                'headers': self.headers
        }


class Input:

    def __init__(
        self, 
        key=None, 
        text=None, 
        expression: str=None, 
        args: List[Any]=None, 
        filepath=None, 
        file=None, 
        path: str=None,
        option: Any=None,
        by_label: bool=False,
        by_value: bool=False
    ) -> None:
        self.key = key
        self.text = text
        self.expression = expression
        self.args = args
        self.filepath = filepath
        self.file = file
        self.path = path
        self.option = option
        self.by_label = by_label
        self.by_value = by_value

    def to_serializable(self):
        return {
                'key': self.key,
                'text': self.text,
                'expression': self.expression,
                'args': self.args,
                'filepath': self.filepath,
                'file': self.file,
                'path': self.path,
                'option': self.option,
                'by_label': self.by_label,
                'by_value': self.by_value
        }


class Options:

    def __init__(self, event: str=None, option=None, is_checked=False, timeout=10, extra={}, switch_by: str='url') -> None:
        self.event = event
        self.option = option
        self.is_checked = is_checked
        self.timeout = timeout
        self.extra = extra
        self.switch_by = switch_by

    def to_serializable(self):
        return {
            'event': self.event,
            'option': self.option,
            'is_checked': self.is_checked,
            'timeout': self.timeout,
            'extra': self.extra,
            'switch_by': self.switch_by
        }

class PlaywrightCommand:
    
    def __init__(self, 
        name, 
        command, 
        page: Page = Page(), 
        url: URL = URL(), 
        input: Input = Input(), 
        options: Options = Options(), 
        user: str = None, 
        tags: List[Dict[str, str]] = []
    ) -> None:
        self.name = name
        self.command = command
        self.page = page
        self.url = url
        self.input = input
        self.options = options
        self.metadata = Metadata(user, tags)
        self.hooks = Hooks()

    def to_serializable(self):

        return {
            'name': self.name,
            'command': self.command,
            'page': self.page.to_serializable(),
            'url': self.url.to_serializable(),
            'input': self.input.to_serializable(),
            'options': self.options.to_serializable(),
            'metadata': {
                'user': self.metadata.user,
                'tags': self.metadata.tags
            },
            'hooks': self.hooks.to_names()
        }

    def iter_values(self):
        fields = {
            'name': self.name,
            'command': self.command,
            **self.page.to_serializable(),
            **self.url.to_serializable()
            **self.input.to_serializable(),
            **self.options.to_serializable()
        }
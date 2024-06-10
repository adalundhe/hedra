import uuid
from typing import Dict, List, Any
from hedra.core.engines.types.common.metadata import Metadata
from .hooks import Hooks


class Page:

    __slots__ = (
        'selector',
        'attribute',
        'x_coordinate',
        'y_coordinate',
        'frame'
    )

    def __init__(
        self, 
        selector: str=None, 
        attribute: str=None, 
        x_coordinate: int=0, 
        y_coordinate: int=0, 
        frame=0
    ) -> None:
        self.selector = selector
        self.attribute = attribute
        self.x_coordinate = x_coordinate
        self.y_coordinate = y_coordinate
        self.frame = frame


class URL:

    __slots__ = (
        'location',
        'headers'
    )

    def __init__(
        self, 
        location: str=None, 
        headers: Dict[str, str]={}
    ) -> None:
        self.location = location
        self.headers = headers


class Input:

    __slots__ = (
        'key',
        'text',
        'expression',
        'args',
        'filepath',
        'file',
        'path',
        'option',
        'by_label',
        'by_value'
    )

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


class Options:

    __slots__ = (
        'event',
        'option',
        'is_checked',
        'timeout',
        'extra',
        'switch_by'
    )

    def __init__(
        self, 
        event: str=None, 
        option=None, 
        is_checked=False, 
        timeout=10, 
        extra: Dict[str, Any]={}, 
        switch_by: str='url'
    ) -> None:
        self.event = event
        self.option = option
        self.is_checked = is_checked
        self.timeout = timeout
        self.extra = extra
        self.switch_by = switch_by


class PlaywrightCommand:

    __slots__ = (
        'action_id'
        'name',
        'command',
        'page',
        'url',
        'input',
        'options',
        'metadata',
        'hooks',
        'event',
        'command_args',
        'mutations'
    )
    
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
        self.action_id = str(uuid.uuid4())
        self.name = name
        self.command = command
        self.page = page
        self.url = url
        self.input = input
        self.options = options
        self.metadata = Metadata(user, tags)
        self.hooks: Hooks[PlaywrightCommand] = Hooks()
        self.event = None
        self.command_args: Dict[str, Any] = {}

from typing import List, Type
from .abstract_frame import AbstractFrame
from .alternate_service_frame import AltSvcFrame
from .continuation_frame import ContinuationFrame
from .data_frame import DataFrame
from .extention_frame import ExtensionFrame
from .go_away_frame import GoAwayFrame
from .headers_frame import HeadersFrame
from .ping_frame import PingFrame
from .priority_frame import PriorityFrame
from .push_promise_frame import PushPromiseFrame
from .reset_stream_frame import RstStreamFrame
from .settings_frame import SettingsFrame
from .window_update_frame import WindowUpdateFrame



_FRAME_CLASSES: List[Type[AbstractFrame]] = [
    DataFrame,
    HeadersFrame,
    PriorityFrame,
    RstStreamFrame,
    SettingsFrame,
    PushPromiseFrame,
    PingFrame,
    GoAwayFrame,
    WindowUpdateFrame,
    ContinuationFrame,
    AltSvcFrame,
]
#: FRAMES maps the type byte for each frame to the class used to represent that
#: frame.
FRAMES = {cls.type: cls for cls in _FRAME_CLASSES}
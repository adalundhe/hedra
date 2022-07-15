from enum import IntEnum


class SettingCodes(IntEnum):
    """
    All known HTTP/2 setting codes.

    .. versionadded:: 2.6.0
    """

     #: The byte that signals the SETTINGS_HEADER_TABLE_SIZE setting.
    HEADER_TABLE_SIZE = 0x01
    #: The byte that signals the SETTINGS_ENABLE_PUSH setting.
    ENABLE_PUSH = 0x02
    #: The byte that signals the SETTINGS_MAX_CONCURRENT_STREAMS setting.
    MAX_CONCURRENT_STREAMS = 0x03
    #: The byte that signals the SETTINGS_INITIAL_WINDOW_SIZE setting.
    INITIAL_WINDOW_SIZE = 0x04
    #: The byte that signals the SETTINGS_MAX_FRAME_SIZE setting.
    MAX_FRAME_SIZE = 0x05
    #: The byte that signals the SETTINGS_MAX_HEADER_LIST_SIZE setting.
    MAX_HEADER_LIST_SIZE = 0x06
    #: The byte that signals SETTINGS_ENABLE_CONNECT_PROTOCOL setting.
    ENABLE_CONNECT_PROTOCOL = 0x08


from hedra.core.engines.types.http2.streams.changed_setting import ChangedSetting
from hedra.core.engines.types.http2.streams.stream_settings import SettingCodes
from .base_event import BaseEvent


class RemoteSettingsChanged(BaseEvent):
    event_type='REMOTE_SETTINGS_CHANGED'
    __slots__ = (
        'changed_settings'
    )
    """
    The RemoteSettingsChanged event is fired whenever the remote peer changes
    its settings. It contains a complete inventory of changed settings,
    including their previous values.

    In HTTP/2, settings changes need to be acknowledged. h2 automatically
    acknowledges settings changes for efficiency. However, it is possible that
    the caller may not be happy with the changed setting.

    When this event is received, the caller should confirm that the new
    settings are acceptable. If they are not acceptable, the user should close
    the connection with the error code :data:`PROTOCOL_ERROR
    <h2.errors.ErrorCodes.PROTOCOL_ERROR>`.

    .. versionchanged:: 2.0.0
       Prior to this version the user needed to acknowledge settings changes.
       This is no longer the case: h2 now automatically acknowledges
       them.
    """
    def __init__(self):
        #: A dictionary of setting byte to
        #: :class:`ChangedSetting <h2.settings.ChangedSetting>`, representing
        #: the changed settings.
        self.changed_settings = {}

    @classmethod
    def from_settings(cls, old_settings, new_settings):
        """
        Build a RemoteSettingsChanged event from a set of changed settings.

        :param old_settings: A complete collection of old settings, in the form
                             of a dictionary of ``{setting: value}``.
        :param new_settings: All the changed settings and their new values, in
                             the form of a dictionary of ``{setting: value}``.
        """
        e = cls()
        for setting, new_value in new_settings.items():

            try:
                setting = SettingCodes(setting)
            except ValueError:
                pass

            original_value = old_settings.get(setting)
            change = ChangedSetting(setting, original_value, new_value)
            e.changed_settings[setting] = change

        return e

    def __repr__(self):
        return "<RemoteSettingsChanged changed_settings:{%s}>" % (
            ", ".join(repr(cs) for cs in self.changed_settings.values()),
        )
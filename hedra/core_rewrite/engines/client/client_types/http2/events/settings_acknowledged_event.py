from .base_event import BaseEvent


class SettingsAcknowledged(BaseEvent):
    event_type='SETTINGS_ACKNOWLEDGED'
    __slots__ = ('changed_settings')
    """
    The SettingsAcknowledged event is fired whenever a settings ACK is received
    from the remote peer. The event carries on it the settings that were
    acknowedged, in the same format as
    :class:`h2.events.RemoteSettingsChanged`.
    """
    def __init__(self):
        #: A dictionary of setting byte to
        #: :class:`ChangedSetting <h2.settings.ChangedSetting>`, representing
        #: the changed settings.
        self.changed_settings = {}

    def __repr__(self):
        return "<SettingsAcknowledged changed_settings:{%s}>" % (
            ", ".join(repr(cs) for cs in self.changed_settings.values()),
        )

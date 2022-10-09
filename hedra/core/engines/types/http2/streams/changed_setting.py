class ChangedSetting:

    __slots__ = (
        'setting',
        'original_value',
        'new_value'
    )

    def __init__(self, setting, original_value, new_value):
        #: The setting code given. Either one of :class:`SettingCodes
        #: <h2.settings.SettingCodes>` or ``int``
        #:
        #: .. versionchanged:: 2.6.0
        self.setting = setting

        #: The original value before being changed.
        self.original_value = original_value

        #: The new value after being changed.
        self.new_value = new_value

    def __repr__(self):
        return (
            "ChangedSetting(setting=%s, original_value=%s, "
            "new_value=%s)"
        ) % (
            self.setting,
            self.original_value,
            self.new_value
        )

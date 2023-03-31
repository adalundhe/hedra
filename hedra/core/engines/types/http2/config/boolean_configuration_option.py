class BooleanConfigOption:
    """
    Descriptor for handling a boolean config option.  This will block
    attempts to set boolean config options to non-bools.
    """
    def __init__(self, name):
        self.name = name
        self.attr_name = '_%s' % self.name

    def __get__(self, instance, owner):
        return getattr(instance, self.attr_name)

    def __set__(self, instance, value):
        if not isinstance(value, bool):
            raise ValueError("%s must be a bool" % self.name)
        setattr(instance, self.attr_name, value)
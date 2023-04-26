from hedra.versioning.flags.types.base.flag_type import FlagTypes
from hedra.versioning.flags.types.base.registry import flag_registrar


@flag_registrar(FlagTypes.UNSTABLE_FEATURE)
def unstable(feature):

    def wrap_feature(*args, **kwargs):

        return feature(*args, **kwargs)
    
    return wrap_feature


@flag_registrar(FlagTypes.UNSTABLE_FEATURE)
def unstable_threadsafe(feature):
    return feature
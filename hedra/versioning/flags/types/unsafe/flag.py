from hedra.versioning.flags.types.base.flag_type import FlagTypes
from hedra.versioning.flags.types.base.registry import flag_registrar


@flag_registrar(FlagTypes.UNSAFE_FEATURE)
def unsafe(feature):

    def wrap_feature(*args, **kwargs):

        return feature(*args, **kwargs)
    
    return wrap_feature
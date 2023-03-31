import inspect
from typing import Dict
from hedra.versioning.flags.types.unsafe.feature import UnsafeFeature
from hedra.versioning.flags.types.unstable.feature import UnstableFeature
from .active import active_flags
from .feature import Flag
from .flag_type import FlagTypes


class FlagRegistry:
    all: Dict[str, Flag] = {}
    module_paths: Dict[str, str] = {}

    def __init__(self, flag_type) -> None:
        self.flag_type = flag_type
        self.flag_types = {
            FlagTypes.UNSAFE_FEATURE: lambda *args, **kwargs: UnsafeFeature(
                *args, 
                **kwargs
            ),
            FlagTypes.UNSTABLE_FEATURE: lambda *args, **kwargs: UnstableFeature(
                *args, 
                **kwargs
            )
        }

    def __call__(self, flag):

        self.module_paths[flag.__name__] = flag.__module__

        def wrap_feature(feature):
            
            feature_name = feature.__name__

            flagged_feature: Flag = self.flag_types[self.flag_type]

                    
            self.all[feature_name] = flagged_feature(
                feature_name,
                feature
            )

            def wrapped_method(*args, **kwargs):

                selected_feature = self.all.get(feature_name)


                if active_flags.get(selected_feature.type):
                    selected_feature.enabled = True

                else:
                    raise selected_feature.exception(feature_name)

                return selected_feature.feature(*args, **kwargs)
            
            return wrapped_method

        return wrap_feature


def makeRegistrar():
    return FlagRegistry


flag_registrar = makeRegistrar()
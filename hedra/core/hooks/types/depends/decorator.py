from .validator import DependsValidator


def depends(*stages):
    DependsValidator(stages=stages)

    def wrapper(cls):

        def decorator():

            direct_dependencies = [stage for stage in stages]
            cls.dependencies = direct_dependencies
            
            return cls

        return decorator()

    return wrapper
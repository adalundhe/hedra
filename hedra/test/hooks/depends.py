def depends(*stages):

    def wrapper(cls):

        def decorator():

            direct_dependencies = [stage for stage in stages]
            cls.dependencies = direct_dependencies

            all_dependencies = [stage for stage in stages]
            
            for stage in stages:
                all_dependencies.extend(stage.dependencies)

            cls.all_dependencies = list(set(all_dependencies))

            return cls

        return decorator()

    return wrapper
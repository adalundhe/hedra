from typing import List
from hedra.test.stages.stage import Stage
from hedra.test.stages.types.stage_types import StageTypes


def validate_direct_dependents(instance: Stage, instance_type: StageTypes):
    return len([
        dependency for dependency in instance.dependencies if dependency.stage_type == instance_type
    ]) > 0


def validate_all_dependents(instance: Stage, instance_type: StageTypes):
    return len([
        dependency for dependency in instance.all_dependencies if dependency.stage_type == instance_type
    ]) > 0

def validate_no_such_dependents(instance: Stage, instance_type: StageTypes):
    validated_dependencies = list(set(
        [
            dependency.__name__ for dependency in instance.dependencies if dependency.stage_type == instance_type
        ]
    ))
    
    if len(validated_dependencies) > 0:
        return False, validated_dependencies

    else:
        return True, validated_dependencies

def validate_dependents_match_type(instance: Stage, instance_type: StageTypes):
    validated_dependencies = list(set(
        [
            dependency.__name__ for dependency in instance.dependencies if dependency.stage_type != instance_type
        ]
    ))
    
    if len(validated_dependencies) > 0:
        return False, validated_dependencies

    else:
        return True, validated_dependencies

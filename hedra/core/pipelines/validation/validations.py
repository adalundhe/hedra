from .dependencies import (
    validate_all_dependents,
    validate_direct_dependents,
    validate_no_such_dependents,
    validate_dependents_match_type
)

from .exceptions import (
    MissingDependentsError,
    UnexpectedDependencyTypeError
)

from hedra.test.stages.types import StageTypes


def validate_setup_stage(instance):
    no_setup_deps, invalid_dependents = validate_no_such_dependents(
        instance,
        StageTypes.SETUP
    )

    if no_setup_deps is False:
        raise UnexpectedDependencyTypeError(
            StageTypes.SETUP,
            StageTypes.SETUP,
            instance,
            invalid_dependents
        )

    no_optimize_deps, invalid_dependents = validate_no_such_dependents(
        instance,
        StageTypes.OPTIMIZE
    )

    if no_optimize_deps is False:
        raise UnexpectedDependencyTypeError(
            StageTypes.SETUP,
            StageTypes.OPTIMIZE,
            instance,
            invalid_dependents
        )


def validate_optimize_stage(instance):

    has_setup_deps = validate_all_dependents(
        instance,
        StageTypes.SETUP
    )
    if has_setup_deps is False:
        raise MissingDependentsError(
            StageTypes.OPTIMIZE,
            StageTypes.SETUP,
            instance
        )

    no_optimize_deps, invalid_dependents = validate_no_such_dependents(
        instance,
        StageTypes.OPTIMIZE
    )

    if no_optimize_deps is False:
        raise UnexpectedDependencyTypeError(
            StageTypes.OPTIMIZE,
            StageTypes.OPTIMIZE,
            instance,
            invalid_dependents
        )

    no_analysis_deps, invalid_dependents = validate_no_such_dependents(
        instance,
        StageTypes.ANALYIZE
    )

    if no_analysis_deps is False:
        raise UnexpectedDependencyTypeError(
            StageTypes.OPTIMIZE,
            StageTypes.ANALYIZE,
            instance,
            invalid_dependents
        )


def validate_execute_stage(instance):
    has_setup = validate_all_dependents(
        instance,
        StageTypes.SETUP

    )
    
    if has_setup is False:
        raise MissingDependentsError(
            StageTypes.EXECUTE,
            StageTypes.SETUP,
            instance
        )


def validate_teardown_stage(instance):
    has_execute_dependency, invalid_dependents = validate_direct_dependents(
        instance,
        StageTypes.EXECUTE
    )

    if has_execute_dependency is False:
        raise MissingDependentsError(
            StageTypes.TEARDOWN,
            StageTypes.EXECUTE,
            instance
        )

    no_teardown_deps, invalid_dependents = validate_no_such_dependents(
        instance,
        StageTypes.TEARDOWN
    )

    if no_teardown_deps is False:
        raise UnexpectedDependencyTypeError(
            StageTypes.CHECKPOINT,
            StageTypes.CHECKPOINT,
            instance,
            invalid_dependents
        )


def validate_analyze_stage(instance):

    all_dependencies_are_execute, invalid_dependents = validate_dependents_match_type(
        instance,
        StageTypes.EXECUTE
    )

    if all_dependencies_are_execute is False:
        raise UnexpectedDependencyTypeError(
            StageTypes.ANALYIZE,
            StageTypes.EXECUTE,
            instance,
            invalid_dependents
        )

    no_analyze_deps, invalid_dependents = validate_no_such_dependents(
        instance,
        StageTypes.ANALYIZE
    )

    if no_analyze_deps is False:
        raise UnexpectedDependencyTypeError(
            StageTypes.ANALYIZE,
            StageTypes.ANALYIZE,
            instance,
            invalid_dependents
        )


def validate_checkpoint_stage(instance):

    all_dependencies_are_analyze, invalid_dependents = validate_dependents_match_type(
        instance,
        StageTypes.ANALYIZE
    )

    if all_dependencies_are_analyze is False:
        raise UnexpectedDependencyTypeError(
            StageTypes.CHECKPOINT,
            StageTypes.ANALYIZE,
            instance,
            invalid_dependents
        )

    no_checkpoint_deps, invalid_dependents = validate_no_such_dependents(
        instance,
        StageTypes.CHECKPOINT
    )

    if no_checkpoint_deps is False:
        raise UnexpectedDependencyTypeError(
            StageTypes.CHECKPOINT,
            StageTypes.CHECKPOINT,
            instance,
            invalid_dependents
        )

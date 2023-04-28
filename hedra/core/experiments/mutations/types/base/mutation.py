from typing import Tuple, Any
from types import SimpleNamespace
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.engines.types.common.base_action import BaseAction
from hedra.versioning.flags.types.unstable.flag import unstable_threadsafe
from .mutation_type import MutationType
from .validator import MutationValidator



class Mutation:

    def __init__(
        self,
        name: str,
        chance: float,
        mutation_type: MutationType,
        *targets: Tuple[str],
    ) -> None:
        validated_mutation = MutationValidator(
            name=name,
            chance=chance,
            targets=targets,
            mutation_type=mutation_type
        )

        self.name = validated_mutation.name
        self.chance = validated_mutation.chance
        self.targets = list(validated_mutation.targets)
        self.stage: Any = SimpleNamespace(
            context=SimpleContext()
        )

        self.mutation_type = validated_mutation.mutation_type

        unstable_threadsafe(Mutation)

    async def mutate(self, action: BaseAction=None):
        raise NotImplementedError(
            'Err. - mutate() is an abstract method in the base Mutation class.'
        )
    
    def copy(self):
        raise NotImplementedError(
            'Err. - copy() is an abstract method in the base Mutation class.'
        )
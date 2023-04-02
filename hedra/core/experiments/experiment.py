import math
from typing import List, Dict
from hedra.versioning.flags.types.unstable.flag import unstable
from .variant import Variant


@unstable
class Experiment:

    def __init__(
        self,
        experiment_name: str,
        participants: List[Variant]
    ) -> None:
        self.experiment_name = experiment_name
        self.participants: Dict[str, Variant] = {}
        self.source_batch_size: int = 0

        total_weight = 1.0
        missing_weight_participants = []

        for participant in participants:
            
            if participant.weight:
                total_weight -= participant.weight

            else:
                missing_weight_participants.append(participant.stage_name)

            self.participants[participant.stage_name] = participant

        per_participant_weight = 0
        if len(missing_weight_participants) > 0:
            per_participant_weight = total_weight/len(missing_weight_participants)
            
        for missing_weight_participant in missing_weight_participants:
            self.participants[missing_weight_participant].weight = per_participant_weight

    def is_variant(self, stage_name: str) -> bool:
        variant = self.participants.get(stage_name)
        return variant is not None

    def get_variant_batch_size(self, stage_name: str) -> int:
        variant = self.participants.get(stage_name)

        if variant is None:
            raise Exception(
                f'Err. - requested variant {stage_name} is not a participant in experiment {self.experiment_name}.'
            )

        return math.ceil(variant.weight * self.source_batch_size)
        
        
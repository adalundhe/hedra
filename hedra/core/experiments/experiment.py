import math
import random
from typing import List, Dict, Union, Iterable
from hedra.versioning.flags.types.unstable.flag import unstable
from .variant import Variant


@unstable
class Experiment:

    def __init__(
        self,
        experiment_name: str,
        participants: List[Variant],
        random: bool=True
    ) -> None:
        self.experiment_name = experiment_name
        self.participants: Dict[str, Variant] = {
            participant.stage_name: participant for participant in participants
        }
        self.source_batch_size: int = 0
        self.random: bool = random
        self.participants_count = len(participants)

    def __iter__(self):
        for participant in self.participants.values():
            yield participant

    def assign_weights(self):

        total_weight = 1.0
        missing_weight_participants = []

        for participant_idx, participant in enumerate(self.participants.values()):
            
            if participant.weight:
                total_weight -= participant.weight

            elif self.random:
                weight  = round(random.uniform(0.1, 0.9), 2)

                if participant_idx < self.participants_count - 1:
                    participant.weight = weight
                    total_weight -= weight

                else:
                    participant.weight = round(total_weight, 2)

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
    
    def get_variant(self, stage_name: str) -> Union[Variant, None]:
        if self.is_variant(stage_name):
            variant = self.participants.get(stage_name)

            return variant
        
    
    def calculate_distribution(
        self,
        stage_name: str,
        batch_size: int
    ) -> Union[List[float], None]:
        variant = self.get_variant(stage_name)

        if variant.distribution:
            return variant.distribution.generate(batch_size)

        
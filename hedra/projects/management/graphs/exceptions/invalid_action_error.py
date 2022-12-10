from typing import List


class InvalidActionError(Exception):

    def __init__(self, specified_workflow_action: str, valid_workflow_actions: List[str]) -> None:

        valid_action_names = '\n-'.join(valid_workflow_actions)

        super().__init__(
            f'\n\nError - Invalid workflow action - {specified_workflow_action} - specified. Valid actions are:\n\n{valid_action_names}\n'
        )
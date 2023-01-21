from pydantic import BaseModel, Field


class SaveValidator(BaseModel):
    key: str=Field(..., min_length=1)
    checkpoint_filepath: str=Field(..., min_length=1)

    def __init__(__pydantic_self__, key: str, checkpoint_filepath: str) -> None:
        super().__init__(
            key=key,
            checkpoint_filepath=checkpoint_filepath
        )
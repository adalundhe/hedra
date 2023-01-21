from pydantic import BaseModel, Field, StrictStr


class SaveHookValidator(BaseModel):
    key: StrictStr=Field(..., min_length=1)
    checkpoint_filepath: StrictStr=Field(..., min_length=1)


class SaveValidator:

    def __init__(__pydantic_self__, key: str, checkpoint_filepath: str) -> None:
        SaveHookValidator(
            key=key,
            checkpoint_filepath=checkpoint_filepath
        )
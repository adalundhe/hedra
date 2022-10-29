
from __future__ import annotations
import inspect
from typing import Any, Generic, Optional, TypeVar

from pydantic import create_model

T = TypeVar('T')


class ReporterConfig(Generic[T]):
    reporter_type: Optional[str]

    def __init__(self, **data: Any) -> None:
        super().__init__()

        attributes = inspect.getmembers(
            self, 
            lambda attr: not(inspect.isroutine(attr))
        )

        instance_attributes = [
            attr for attr in attributes if not(
                attr[0].startswith('__') and attr[0].endswith('__')
            )
        ]

        base_attributes = set(dir(ReporterConfig))
        
        model_attributes = {}
        for attribute_name, attribute_value in instance_attributes:
            
            if attribute_name not in base_attributes:
                model_attributes[attribute_name] = attribute_value

        model_attributes.update(data)

        for attribute_name, attribute_value in data.items():
            setattr(self, attribute_name, attribute_value)

        validation_model = create_model(
            self.__class__.__name__,
            **{
                field_name: (
                    field_type, 
                    ...
                ) for field_name, field_type in self.__class__.__annotations__.items()
            }
        )
        
        validation_model(**model_attributes)
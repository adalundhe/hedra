from __future__ import annotations
from typing import Any, Optional, List, Union, Dict


class SimpleContext:

    def __init__(self, **kwargs) -> None:

        self.known_keys = [
            'stages',
            'visited',
            'results',
            'results_stages',
            'summaries',
            'paths',
            'path_lengths',
            'known_keys'
        ]

        self.ignore_serialization_filters = []

        for kwarg_name, kwarg in kwargs.items():
            object.__setattr__(self, kwarg_name, kwarg)

    def __iter__(self):

        ignore_items = [
            *self.known_keys,
            *self.ignore_serialization_filters
        ]

        for key, value in self.__dict__.items():
            if key.startswith('__') is False and key not in ignore_items:
                yield key, value

    def __getattribute__(self, __name: str) -> Any:
        return object.__getattribute__(self, __name)

    def __setattr__(self, __name: str, __value: Any) -> None:
        object.__setattr__(self, __name, __value)

    def __getitem__(self, name: str) -> Optional[Any]:
        if hasattr(self, name):
            return object.__getattribute__(self, name)

        return None

    def __setitem__(self, name: str, value: Any) -> None:
        object.__setattr__(self, name, value)

    def get(self, name: str) -> Optional[Any]:
        return self.__getitem__(name)       

    def keys(self) -> List[str]: 
        return [key for key in self.__dict__.keys() if key.startswith('__') is False]

    def values(self) -> List[Any]:
        return [value for key, value in self.__dict__.items() if key.startswith('__') is False]
    
    def items(self):
        return [
            (
                key, 
                value
            ) for key, value in self.__dict__.items() if key.startswith('__') is False
        ]

    def remove(self, name: str):
        if name.startswith('__') is False:
            object.__delattr__(self, name)

    def update(self, update_context: Union[SimpleContext, Dict[str, Any]]):
        for context_key, context_value in update_context.items():
            object.__setattr__(self, context_key, context_value)

    def as_serializable(self):

        ignore_items = [
            *self.known_keys,
            *self.ignore_serialization_filters
        ]

        serialization_items = []
        for key, value in self.__dict__.items():
            if key.startswith('__') is False and key not in  ignore_items:
                serialization_items.append((
                    key,
                    value
                ))
        
        return serialization_items

    def create_or_update(
        self,
        context_key: str,
        value: Any,
        default: Any
    ):

        if hasattr(self, context_key):
            if isinstance(value, dict):
                exitsting_value: dict = self.__getitem__(context_key)
                exitsting_value.update(value)

                self.__setitem__(context_key, exitsting_value)

            elif isinstance(value, list):
                exitsting_value: list = self.__getitem__(context_key)
                exitsting_value.extend(value)

                self.__setitem__(context_key, exitsting_value)
            
            else:
                self.__setitem__(context_key, value)

        else:
            self.__setitem__(context_key, default)

    def create_if_not_exists(
        self,
        context_key: str,
        value: Any
    ):

        if hasattr(self, context_key) is False:
            self.__setitem__(context_key, value)
    
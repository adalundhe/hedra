from __future__ import annotations
from functools import reduce
from typing import List, Any, Union
from typing import Any, AsyncIterable
from hedra.tools.helpers import (
    awaitable,
    wrap
)


class AsyncList:

    def __init__(self, data=[]):
        self.data = data

    def __getitem__(self, start: int, stop: int=None) -> Union[AsyncList, Any]:
        if stop:
            subset_list = self.data[start:stop]
            return AsyncList(subset_list)

        return self.data[start]

    def __setitem__(self, index: int, value: Any) -> None:
        self.data[index] = value

    def __add__(self, value: Any) -> List[Any]:
        appended_list = [*self.data, value]
        return AsyncList(appended_list)

    def __mul__(self, value: int) -> AsyncList:
        multiplied_list = self.data * value
        return AsyncList(multiplied_list)

    def __rmul__(self, value: int) -> AsyncList:
        multiplied_list = self.data * value
        return AsyncList(multiplied_list)

    def __iter__(self) -> AsyncIterable:
        for item in self.data:
            yield item

    async def __aiter__(self) -> AsyncIterable:
        for item in self.data:
            yield item

    async def enum(self) -> AsyncIterable:
        for idx, item in enumerate(self.data):
            yield idx, item

    async def size(self) -> int:
        return await awaitable(len, self.data)

    async def is_empty(self) -> bool:
        list_size = await self.size()
        return list_size == 0

    async def append(self, value) -> None:
        self.data = [
            *self.data,
            value
        ]

    async def prepend(self, value) -> None:
        self.data = [
            value,
            self.data
        ]

    async def insert(self, idx, value) -> None:
        size = await self.size(self.data)
        if idx >= size:
            await self.append(value)

        elif idx < 0:
            await self.prepend(value)

        else:
            self.data = [
                self.data[:idx-1],
                value,
                self.data[idx:]
            ]

    async def at(self, idx) -> Any:
        size = await self.size(self.data)
        if idx >= size:
            return None
        
        return self.data[idx]

    async def find(self, value) -> Any:
        for item in self.data:
            if item == value:
                return value

        return None

    async def exists(self, value) -> bool:
        found = await self.find(value)
        return found is not None

    async def sort(self, **kwargs) -> AsyncList:
        sorted_list = await awaitable(sorted, self.data, **kwargs)
        return AsyncList(sorted_list)

    async def filter(self, condition) -> AsyncList:
        filtered = []
        for item in self.data:
            meets_condition = await awaitable(condition, item)
            if meets_condition:
                filtered = [
                    *filtered,
                    item
                ]

        return AsyncList(filtered)

    async def map(self, transform) -> AsyncList:
        mapped = []
        for item in self.data:
            mapped_value = await awaitable(transform, item)
            mapped = [*mapped, mapped_value]

        return AsyncList(mapped)

    async def subscribe(self, hook, *args, **kwargs) -> None:
        async_hook = await wrap(hook, args, kwargs)
        async for item in async_hook():
            self.data = [
                *self.data,
                item
            ]

    async def publish(self, hook) -> None:
        for item in self.data:
            await awaitable(hook, item)

    async def join(self, join_character) -> str:
        return await awaitable(self.data.join, join_character)

    async def replace(self, match_value: Any, replacement: Any, occurences: int=-1, in_place=True) -> Union[AsyncList, None]:
        if occurences == -1:
            occurences = await self.size()

        if in_place:
            await self._replace_in_place(match_value, replacement, occurences)
        else:
            return await self._replace(match_value, replacement, occurences)
            
    async def _replace_in_place(self, match_value: Any, replacement: Any, occurences: int):    
        found = 0
        for idx, item in enumerate(self.data):
                if item == match_value and found < occurences:
                    self.data[idx] = replacement
                    found += 1

    async def _replace(self, match_value: Any, replacement: Any, occurences: int):
        replaced_list = AsyncList(list(self.data))
            
        found = 0
        for idx, item in enumerate(self.data):
                if item == match_value and found < occurences:
                    replaced_list[idx] = replacement
                    found += 1

        return replaced_list

    async def index(self, match_item: Any, offset: int=0, end: int=None):
        try:
            return await awaitable(self.data.index, match_item, offset, end)
        except ValueError:
            return -1

    async def indexes(self, match_item: Any) -> List[int]:

        occurences = []
        for idx in range(self.data):
            if self.data[idx] == match_item:
                occurences = [
                    *occurences,
                    idx
                ]

        return occurences

    async def count(self, match_item: Any) -> int:
        return await awaitable(self.data.count, match_item)

    async def reverse(self) -> None:
        await awaitable(self.data.reverse)

    async def pop(self) -> Any:
        return await awaitable(self.data.pop)

    async def remove(self, value: int) -> None:
        await awaitable(self.data.remove, value)

    async def remove_index(self, index: int) -> AsyncList:
        removed = self.data[index:index+1]
        return AsyncList(removed)

    async def remove_range(self, start: int=0, stop: int=None) -> AsyncList:
        if stop is None:
            stop = await self.size()
            return AsyncList(self.data[start:])
        elif start == 0:
            return AsyncList(self.data[:stop])
        else:
            removed = self.data[:start] + self.data[stop:]
            return AsyncList(removed)

    async def copy(self) -> AsyncList:
        return AsyncList(list(self.data))

    async def extend(self, value: List[Any]) -> None:
        await awaitable(self.data.extend, value)

    async def splice(self, start: int=0, stop: int=None, step=1) -> AsyncList:
        spliced = []
        for idx in range(start, stop, step):
            spliced = [
                *spliced,
                self.data[idx]
            ]

        return AsyncList(spliced)

    async def subset(self, values: List[Any]) -> AsyncList:

        found = []
        for value in values:
            found_item = await self.find(value)
            found = [
                *found,
                found_item
            ]

        return AsyncList(found)

    async def reduce(self, reducer: function) -> AsyncList:
        reduced = await awaitable(reduce, reducer, self.data)
        return AsyncList(reduced)

    async def maximum(self, *args, **kwargs):
        return await awaitable(max, self.data, *args, **kwargs)

    async def minimum(self, *args, **kwargs):
        return await awaitable(min, self.data, *args, **kwargs)

    async def clear(self) -> None:
        await awaitable(self.data.clear)

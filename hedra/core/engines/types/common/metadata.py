from typing import Dict, List, Optional


class Metadata:

    __slots__ = (
        'user',
        'tags'
    )

    def __init__(self, user: Optional[str] = None, tags: List[Dict[str, str]] = []) -> None:
        self.user = user
        self.tags = tags

    def __aiter__(self):
        for tag in self.tags:
            yield tag

    def update_tags(self, user: str=None, tags: List[Dict[str, str]]=[]):
        if user:
            self.user = user
            
        self.tags.extend(tags)

    def tags_to_string_list(self):
        return [
           f'{tag.get("name")}:{tag.get("value")}'  for tag in self.tags
        ]
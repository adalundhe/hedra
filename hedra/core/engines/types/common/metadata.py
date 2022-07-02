from typing import Dict, List, Optional


class Metadata:

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
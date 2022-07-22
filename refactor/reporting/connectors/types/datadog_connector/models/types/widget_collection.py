from __future__ import annotations
from .widget import Widget


class WidgetCollection:

    def __init__(self, widgets=None):

        if widgets:
            self.widgets = [
                Widget(widget) for widget in widgets
            ]

        else:
            self.widgets = []

        if len(self.widgets) > 0:
            self.not_empty = True
        
        else:
            self.not_empty = False

    def __iter__(self):
        for widget in self.widgets:
            yield widget
    
    async def get(self, name) -> Widget:
        match = None
        for widget in self.widgets:
            if widget.name == name:
                match = widget

        return match

    async def update(self, update_widget_collection) -> WidgetCollection:
        for i, update_widget in enumerate(update_widget_collection):
            widget = await self.get(update_widget.name)
            await widget.update(update_widget)
            self.widgets[i] = widget

        return self

    async def from_datadog_widget_list(self, widget_list) -> WidgetCollection:

        if len(widget_list) > 0:
            self.not_empty = True
        else:
            self.not_empty = False

        for widget_dict in widget_list:
            widget = Widget({})
            await widget.from_datadog_widget_dict(widget_dict)

            self.widgets += [widget]

        return self

    async def to_dict_list(self) -> list:
        return [
            await widget.to_dict() for widget in self.widgets
        ]

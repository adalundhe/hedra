from __future__ import annotations


class Widget:

    def __init__(self, widget):
        self.id = widget.get('id')
        self.name = widget.get('name')
        self.type = widget.get('type')
        self.field_config = widget.get('field_config')
        self.options = widget.get('options', {})
        self._layout_options = self.options.get('layout')

    async def from_datadog_widget_dict(self, datadog_widget_dict) -> Widget:

        widget_definition = datadog_widget_dict.get('definition')
        self.id = widget_definition.get('id')
        self.name = widget_definition.get('title')
        self.type = widget_definition.get('type')

        self.field_config = [
            field.get('q') for field in widget_definition.get('requests', [])
        ]

        return self

    async def update(self, widget) -> Widget:

        if self.id is None:
            self.id = widget.id
        
        if self.name is None:
            self.name = widget.name

        if self.type is None:
            self.type = widget.type

        if widget.field_config:
            self.field_config = list(
                set([
                    *self.field_config,
                    *widget.field_config
                ])
            )

        if len(widget.options) > 0:
            self.options.update(widget.options)

        return self

    async def to_dict(self) -> dict:

        widget_dict = {
            'definition': {
                'type': self.type,
                'title': self.name,
                'requests': [
                    {'q': field} for field in self.field_config
                ]
            }
        }

        if self.id:
            widget_dict['id'] = self.id

        if self._layout_options:
            widget_dict['layout'] = self._layout_options

        return widget_dict
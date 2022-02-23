from __future__ import annotations
import datadog
from .widget_collection import WidgetCollection
from async_tools.functions import awaitable


class Dashboard:

    def __init__(self, dashboard):
        self.id = dashboard.get('id')
        self.name = dashboard.get('name')
        self.layout_type = dashboard.get('layout_type', 'ordered')
        self.description = dashboard.get('description')
        self.widgets = WidgetCollection(
            widgets=dashboard.get('widgets', [])
        )
        self.options = dashboard.get('options', {})

        self._read_only = self.options.get('read_only', False)
        self._author = self.options.get('author', '')
        self._notify_list = self.options.get('notify_list')
        self._views = self.options.get('views', [])
        self._template_variables = self.options.get('template_variables')

        self._fields = {
            'id': 'id',
            'name': 'title',
            'layout_type': 'layout_type',
            'description': 'description',
            '_read_only': 'is_read_only',
            '_author': 'author_handle',
            '_notify_list': 'notify_list',
            '_template_variables': 'template_variables',
            '_views': 'template_variable_presets'
        }

    async def get(self) -> dict:
        if self.options.get('all'):
            response = await awaitable(datadog.api.Dashboard.get_all)
        else:
            response = await awaitable(datadog.api.Dashboard.get, self.id)

        return response

    async def create(self) -> dict:
        response = await awaitable(
            datadog.api.Dashboard.create,
            title=self.name,
            layout_type=self.layout_type,
            description=self.description,
            widgets=await self.widgets.to_dict_list(),
            is_read_only=self._read_only,
            author_handle=self._author,
            notify_list=self._notify_list,
            template_variables=self._template_variables,
            template_variable_presets=self._views
        )

        return response

    async def update(self) -> dict:

        if self.options.get('get_and_update'):
            dashboard = await self._update_dashboard_from_existing()
            await self._update_widgets(dashboard)

        else:
            dashboard = await self._assemble_monitor_request()

        if self.widgets.not_empty:
            dashboard['widgets'] = await self.widgets.to_dict_list()
        
        response = await awaitable(
            datadog.api.Dashboard.update,
            **dashboard
        )

        return response

    async def delete(self) -> dict:
        response = await awaitable(
            datadog.api.Dashboard.delete,
            self.id
        )

        return response

    async def _assemble_monitor_request(self) -> dict:
        update = {}
        
        for model_field, datadog_field in self._fields.items():
            value = getattr(self, model_field)

            if value:
                update[datadog_field] = value

        return update

    async def _update_dashboard_from_existing(self) -> dict:
        dashboard = self.get()

        updated_dashboard = await self._assemble_monitor_request()

        await awaitable(
            dashboard.update,
            updated_dashboard
        )

        return dashboard


    async def _update_widgets(self, dashboard) -> Dashboard:
        existing_widget_collection = WidgetCollection()
        await existing_widget_collection.from_datadog_widget_list(
            dashboard.get('widgets')
        )

        await self.widgets.update(existing_widget_collection)

        return self






            
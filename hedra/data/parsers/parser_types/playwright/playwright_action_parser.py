import uuid
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.playwright import (
    PlaywrightCommand,
    Page,
    Input,
    URL,
    Options,
    MercuryPlaywrightClient,
    ContextConfig
)
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.data.parsers.parser_types.common.base_parser import BaseParser
from hedra.data.parsers.parser_types.common.parsing import normalize_headers
from typing import Any, Coroutine, Dict
from .playwright_action_validator import (
    PlaywrightActionValidator,
    PlaywrightInputValidator,
    PlaywrightOptionsValidator,
    PlaywrightPageValidator,
    PlaywrightURLValidator
)


class PlaywrightActionParser(BaseParser):

    def __init__(
        self,
        config: Config
    ) -> None:
        super().__init__(
            PlaywrightActionParser.__name__,
            config
        )

    async def parse(
        self, 
        action_data: Dict[str, Any],
        stage: str
    ) -> Coroutine[Any, Any, Coroutine[Any, Any, ActionHook]]:
        
        normalized_headers = normalize_headers(action_data)

        generator_action = PlaywrightActionValidator(
            name=action_data.get('name'),
            command=action_data.get('command'),
            page=PlaywrightPageValidator(
                selector=action_data.get('selector'),
                attribute=action_data.get('attribute'),
                x_coordinate=action_data.get('x_coordinate'),
                y_coordinate=action_data.get('y_coordinate'),
                frame=action_data.get('frame')
            ),
            url=PlaywrightURLValidator(
                location=action_data.get('location'),
                headers=normalized_headers
            ),
            input=PlaywrightInputValidator(
                key=action_data.get('key'),
                text=action_data.get('text'),
                expression=action_data.get('expression'),
                args=action_data.get('args'),
                filepath=action_data.get('filepath'),
                file=action_data.get('file'),
                path=action_data.get('path'),
                option=action_data.get('option'),
                by_label=action_data.get('by_label', False),
                by_value=action_data.get('by_value', False)
            ),
            options=PlaywrightOptionsValidator(
                event=action_data.get('event'),
                option=action_data.get('option'),
                is_checked=action_data.get('is_checked', False),
                timeout=action_data.get('timeout'),
                extra=action_data.get('extra'),
                switch_by=action_data.get('switch_by')
            ),
            weight=action_data.get('weight'),
            order=action_data.get('order'),
            user=action_data.get('user'),
            tags=action_data.get('tags', [])
        )

        action = PlaywrightCommand(
            generator_action.name,
            generator_action.command,
            url=URL(**generator_action.url.dict(
                exclude_none=True
            )),
            page=Page(**generator_action.page.dict(
                exclude_none=True
            )),
            input=Input(**generator_action.input.dict(
                exclude_none=True
            )),
            options=Options(**generator_action.options.dict(
                exclude_none=True
            ))
        )

        session = MercuryPlaywrightClient(
            concurrency=self.config.batch_size,
            timeouts=self.timeouts,
            group_size=self.config.group_size
        )

        await session.setup(ContextConfig(
            browser_type=self.config.browser_type,
            device_type=self.config.device_type,
            locale=self.config.locale,
            geolocation=self.config.geolocation,
            permissions=self.config.permissions,
            color_scheme=self.config.color_scheme,
            options=self.config.playwright_options
        ))

        hook = ActionHook(
            f'{stage}.{generator_action.name}',
            generator_action.name,
            None
        )

        hook.session = session
        hook.action = action
        hook.stage = stage
        hook.context = SimpleContext()
        hook.hook_id = uuid.uuid4()
      
        hook.metadata.order = generator_action.order
        hook.metadata.weight = generator_action.weight
        hook.metadata.tags = generator_action.tags
        hook.metadata.user = generator_action.user

        return hook




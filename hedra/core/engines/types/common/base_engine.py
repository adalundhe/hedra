import asyncio
from typing import TypeVar, List, Dict, Any, Generic, Coroutine, Callable


A = TypeVar('A')
R = TypeVar('R')


class BaseEngine(Generic[A, R]):

    __slots__ = (
        'waiter'
    )

    def __init__(self) -> None:
        super().__init__()

        self.waiter: asyncio.Future = None


    async def wait_for_active_threshold(self):
        if self.waiter is None:
            self.waiter = asyncio.get_event_loop().create_future()
            await self.waiter

    async def execute_before(self, action: A) -> Coroutine[Any, Any, A]:
        action.action_args = {
            'action': action
        }
        
        for before_batch in action.hooks.before:
            results: List[Dict[str, Any]] = await asyncio.gather(*[
                before.call(**{
                    name: value for name, value in action.action_args.items() if name in before.params
                }) for before in before_batch
            ])

            for before_event, result in zip(before_batch, results):
                for data in result.values():
                    if isinstance(result, dict):
                        action.action_args.update(data)

                    else:
                        action.action_args.update({
                            before_event.shortname: data
                        })

        return action

    async def execute_after(self, action: A, response: R) -> Coroutine[Any, Any, R]:
        action.action_args['action'] = action
        action.action_args['result'] = response

        if action.hooks.notify:
            action.action_args.update({
                name: action_or_task.action for name, action_or_task in action.hooks.listeners.items()
            })

        for after_batch in action.hooks.after:
            results: List[Dict[str, Any]] = await asyncio.gather(*[
                after.call(**{
                    name: value for name, value in action.action_args.items() if name in after.params
                }) for after in after_batch
            ])

            for after_event, result in zip(after_batch, results):
                for data in result.values():
                    if isinstance(result, dict):
                        action.action_args.update(data)

                    else:
                        action.action_args.update({
                            after_event.shortname: data
                        })
 
        return response
    
    async def execute_checks(self, action: A, response: R) -> Coroutine[Any, Any, R]:
        action.action_args['action'] = action
        action.action_args['result'] = response

        if response.error:
            return response

        if action.hooks.notify:
            action.action_args.update({
                name: action_or_task.action for name, action_or_task in action.hooks.listeners.items()
            })
            
        for check_batch in action.hooks.checks:
            results: List[Dict[str, Any]] = await asyncio.gather(*[
                check.call(**{
                    name: value for name, value in action.action_args.items() if name in check.params
                }) for check in check_batch
            ])

            for check_event, result in zip(check_batch, results):
                for data in result.values():
                    if isinstance(result, dict):
                        action.action_args.update(data)

                    else:
                        action.action_args.update({
                            check_event.shortname: data
                        })

        return action.action_args.get('result')
import asyncio
import datetime
from .timer import Timer


class ProgressText:
    def __init__(self):
        self.total_timer = Timer('Total')
        self.group_timer = Timer('Group')

        self.selected_timer_name = self.group_timer.name
        self.selected_timer = self.group_timer

        self.run_cli_task = False
        self.run_timer_task = False
        self.cli_message = ''
        self.cli_messages = []
        self.next_cli_message = 0
        self._cli_task = None
        self._timer_task = None
        self.enabled = True
        self.finalized = False
        self.group_finalized = False

    def __str__(self):

        self.total_timer.update()
        self.group_timer.update()

        if self.finalized:
            return f'{self.cli_message} - {self.total_timer.elapsed_message}'

        elif self.group_finalized:
            return f'{self.cli_message} - {self.group_timer.elapsed_message}'

        return f'> {self.cli_message} - {self.selected_timer.elapsed_message}'

    async def append_cli_message(self, text: str):
        self.cli_message = text
        self.cli_messages.append(text)
        await asyncio.sleep(1)

    def start_cli_tasks(self):
        if self.enabled:
            self.finalized = False
            self.group_finalized = False
            self._timer_task = asyncio.create_task(self._start_timer_tasks())
            self._cli_task = asyncio.create_task(self._start_cli_tasks())

    async def _start_cli_tasks(self):
        self.run_cli_task = True

        while self.run_cli_task:
            for cli_task in self.cli_messages:

                if self.run_cli_task is False:
                    return

                self.cli_message = cli_task
                await asyncio.sleep(2.5)

    async def _start_timer_tasks(self):
        self.run_timer_task = True
        self.total_timer.update()
        self.group_timer.update()

        while self.run_timer_task:
            if self.selected_timer_name == self.total_timer.name:
                self.selected_timer_name = self.group_timer.name
                self.selected_timer = self.group_timer

            else:
                self.selected_timer_name = self.total_timer.name
                self.selected_timer = self.total_timer

            await asyncio.sleep(2)
            if self.run_timer_task is False:
                return


    async def stop_cli_tasks(self):
        self.run_cli_task = False
        self.run_timer_task = False
        await self._cli_task
        await self._timer_task

        self.cli_messages = []

    async def pause_cli_tasks(self):
        self.run_cli_task = False
        if self._cli_task:
            await self._cli_task

    async def clear_and_replace(self, message: str):

        await self.pause_cli_tasks()
        self.cli_message = message
        self.cli_messages = [message]
        self.start_cli_tasks()

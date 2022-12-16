import asyncio
import datetime


class ProgressText:
    def __init__(self):
        self.text = None
        self.start = datetime.datetime.now()
        self.group_start = datetime.datetime.now()
        self.elapsed = 0
        self.group_elapsed = 0
        self.run_cli_task = False
        self.cli_message = None
        self.cli_messages = []
        self.next_cli_message = 0
        self.cli_text = None
        self._cli_task = None
        self.enabled = True

    def __str__(self):
        current = datetime.datetime.now()
        self.elapsed = round((current - self.start).total_seconds())
        self.group_elapsed = (current - self.group_start).total_seconds()

        time_elapsed_string = f'{self.elapsed}s'

        total_minutes = int(self.elapsed/60)
        if total_minutes > 0:
            time_elapsed_string = f'{total_minutes}m.{time_elapsed_string}'

        total_hours = int(self.elapsed/3600)
            
        return f'{self.cli_message}. Elapsed Execution Time: {time_elapsed_string}'

    def start_group_timer(self):
        self.group_start = datetime.datetime.now()


    def set_initial_message(self, text: str):
        self.cli_text = text
        self.cli_messages = [text]

    async def append_cli_message(self, text: str):
        self.cli_text = text
        self.cli_messages.append(text)
        await asyncio.sleep(2.5)

    def start_cli_tasks(self):
        if self.enabled:
            self._cli_task = asyncio.create_task(self._start_cli_tasks())

    async def _start_cli_tasks(self):
        self.run_cli_task = True

        while self.run_cli_task:
            for cli_task in self.cli_messages:
                self.cli_message = cli_task
                await asyncio.sleep(2.5)

                if self.run_cli_task is False:
                    return

    async def stop_cli_tasks(self):
        self.run_cli_task = False
        await self._cli_task

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

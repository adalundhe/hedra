import asyncio
import datetime


class ProgressText:
    def __init__(self):
        self.text = None
        self.start = datetime.datetime.now()
        self.group_start = 0
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
        self.elapsed = (current - self.start).total_seconds()

        if self.group_start > 0:
            self.group_elapsed = current - self.group_start
            
        return f'{self.cli_message}. Elapsed Execution Time: {round(self.elapsed)}s'

    def start_group_timer(self):
        self.group_start = datetime.datetime.now()

    def reset_group_timer(self):
        self.group_start = 0

    async def append_cli_message(self, text: str):
        self.cli_text = text
        self.cli_messages.append(text)
        await asyncio.sleep(len(self.cli_messages))

    def start_cli_tasks(self):
        if self.enabled:
            self._cli_task = asyncio.create_task(self._start_cli_tasks())

    async def _start_cli_tasks(self):
        self.run_cli_task = True

        while self.run_cli_task:
            for cli_task in self.cli_messages:
                self.cli_message = cli_task
                await asyncio.sleep(len(self.cli_messages) * 1.25)

                if self.run_cli_task is False:
                    return

    async def stop_cli_tasks(self):
        self.run_cli_task = False
        await self._cli_task

        self.cli_messages = []

    async def clear_and_replace(self, message: str):
        self.cli_message = message
        self.cli_messages = [message]
        await asyncio.sleep(1)

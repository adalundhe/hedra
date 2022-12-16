import datetime

class Timer:

    def __init__(self, name: str) -> None:
        self.name = name
        self.elapsed = 0
        self.seconds = 0
        self.minutes = 0
        self.hours = 0
        self.start = datetime.datetime.now()
        self.default_message = 'Pending...'
        self.elapsed_message = self.default_message

    def __str__(self) -> str:
        return self.elapsed_message

    def update(self):
        current = datetime.datetime.now()
        self.elapsed = round((current - self.start).total_seconds())

        self.seconds = self.elapsed%60
        time_elapsed_string = f'{self.seconds}s'

        self.minutes = int(self.elapsed/60)%60
        if self.minutes > 0:
            time_elapsed_string = f'{self.minutes}m.{self.seconds}s'

        self.hours = int(self.elapsed/3600)
        if self.hours > 0:
            time_elapsed_string = f'{self.hours}h.{self.minutes}m.{self.seconds}s'


        self.elapsed_message = f'{self.name} Time Elapsed: {time_elapsed_string}'

    def finalize(self, message: str):
        self.update()
        return f'{message} - {self.elapsed_message}'

    def reset(self):
        self.start = datetime.datetime.now()
        self.elapsed_message = self.default_message
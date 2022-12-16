from __future__ import annotations
import sys
import asyncio
import time
import signal
import functools
import inspect
from enum import Enum
from os import get_terminal_size
from typing import List, Mapping, Any, Dict, Coroutine
from asyncio import Task
from yaspin.core import Yaspin
from yaspin.spinners import Spinners
from aiologger.levels import LogLevel
from aiologger.formatters.base import Formatter
from hedra.logging.spinner import ProgressText
from yaspin.helpers import to_unicode
from .async_logger import AsyncLogger
from .logger_types import LoggerTypes


class LoggerMode(Enum):
    CONSOLE='console'
    SYSTEM='system'


async def default_handler(signame: str,spinner: AsyncSpinner):  # pylint: disable=unused-argument
    """Signal handler, used to gracefully shut down the ``spinner`` instance
    when specified signal is received by the process running the ``spinner``.

    ``signum`` and ``frame`` are mandatory arguments. Check ``signal.signal``
    function for more details.
    """
    await spinner.fail()
    await spinner.stop()


class AsyncSpinner(Yaspin):

    def __init__(
        self, 
        logger_name: str=None,
        logger_type: LoggerTypes=LoggerTypes.SPINNER,
        log_level: LogLevel=LogLevel.NOTSET,
        logger_enabled: bool=True,
        spinner: Spinners=None, 
        text: ProgressText=None, 
        color: str=None, 
        on_color: str=None, 
        attrs: List[str]=None, 
        reversal: bool=False, 
        side: str="left", 
        sigmap: Dict[signal.Signals, Coroutine]=None, 
        timer: bool=False,
        enabled: bool=True
    ):
        super().__init__(
            spinner, 
            text, 
            color, 
            on_color, 
            attrs, 
            reversal, 
            side, 
            sigmap, 
            timer
        )

        self.logger: AsyncLogger = AsyncLogger(
            logger_name=logger_name,
            logger_type=logger_type,
            log_level=log_level,
            logger_enabled=logger_enabled
        )

        self.logger.initialize('%(message)s')
        self.display = text

        self.enabled = enabled
        self.logger_enabled = True
        self.logger_mode = LoggerMode.CONSOLE

        self._stdout_lock = asyncio.Lock()
        self._loop = asyncio.get_event_loop()

    @property
    def console(self):
        if self.logger_mode == LoggerMode.SYSTEM:
            for handler in self.logger.handlers:
                handler.formatter = Formatter('%(message)s')

            self.logger_mode = LoggerMode.CONSOLE

        return self

    @property
    def system(self):
        if self.logger_mode == LoggerMode.CONSOLE:
            for handler in self.logger.handlers:
                handler.formatter = Formatter(
                        '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)d - %(message)s',
                        datefmt='%Y-%m-%dT%H:%M:%S.%Z'
                )

            self.logger_mode = LoggerMode.SYSTEM

        return self

    def append_message(self, message: str) -> Coroutine[None]:
        return self.display.append_cli_message(message)

    def set_default_message(self, message: str) -> Coroutine[None]:
        return self.display.clear_and_replace(message)

    def set_message_at(self, message_index: int, message:str) -> None:
        if message_index < len(self.display.cli_messages):
            self.display.cli_messages[message_index] = message

    def finalize(self):
        self.display.finalized = True

    def group_finalize(self):
        self.display.group_finalized = True

    async def debug(self, message: str, *args: List[Any], **kwargs: Mapping[str, Any]) -> Task:
        
        if self.logger_enabled:
            await self._stdout_lock.acquire()
            await self._clear_line()

            # Ensure output is Unicode

            log_result = await self.logger.debug(
                message,
                *args,
                **kwargs
            )

            self._cur_line_len = 0
            self._stdout_lock.release()

            return log_result

    async def info(self, message: str, *args: List[Any], **kwargs: Mapping[str, Any]) -> Task:

        if self.logger_enabled:
            await self._stdout_lock.acquire()
            await self._clear_line()

            # Ensure output is Unicode

            log_result = await self.logger.info(
                message,
                *args,
                **kwargs
            )

            self._cur_line_len = 0
            self._stdout_lock.release()
            
            return log_result

    async def warning(self, message: str, *args: List[Any], **kwargs: Mapping[str, Any]) -> Task:
        
        if self.logger_enabled:
            await self._stdout_lock.acquire()
            await self._clear_line()

            # Ensure output is Unicode

            log_result = await self.logger.warning(
                message,
                *args,
                **kwargs
            )

            self._cur_line_len = 0
            self._stdout_lock.release()
            return log_result

    async def warn(self, message: str, *args: List[Any], **kwargs: Mapping[str, Any]) -> Task:
        if self.logger_enabled:  
            await self._stdout_lock.acquire()
            await self._clear_line()

            # Ensure output is Unicode

            log_result = await self.logger.warn(
                message,
                *args,
                **kwargs
            )

            self._cur_line_len = 0
            self._stdout_lock.release()
            return log_result

    async def error(self, message: str, *args: List[Any], **kwargs: Mapping[str, Any]) -> Task:
        
        if self.logger_enabled:
            await self._stdout_lock.acquire()
            await self._clear_line()

            # Ensure output is Unicode

            log_result = await self.logger.error(
                message,
                *args,
                **kwargs
            )

            self._cur_line_len = 0
            self._stdout_lock.release()
            return log_result

    async def critical(self, message: str, *args: List[Any], **kwargs: Mapping[str, Any]) -> Task:
        
        if self.logger_enabled:
            await self._stdout_lock.acquire()
            await self._clear_line()

            # Ensure output is Unicode

            log_result = await self.logger.critical(
                message,
                *args,
                **kwargs
            )

            self._cur_line_len = 0
            self._stdout_lock.release()
            return log_result
    
    async def fatal(self, message: str, *args: List[Any], **kwargs: Mapping[str, Any]) -> Task:
        
        if self.logger_enabled:
            await self._stdout_lock.acquire()
            await self._clear_line()

            # Ensure output is Unicode

            log_result = await self.logger.fatal(
                message,
                *args,
                **kwargs
            )

            self._cur_line_len = 0
            self._stdout_lock.release()
            return log_result

    async def __aenter__(self):
        self.display.group_timer.reset()
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, traceback):
        # Avoid stop() execution for the 2nd time
        if self.enabled and self._spin_thread.done() is False and self._spin_thread.cancelled() is False:
            await self.stop()
            
        return False  # nothing is handled

    def __call__(self, fn):
        @functools.wraps(fn)
        async def inner(*args, **kwargs):
            async with self:

                if inspect.iscoroutinefunction(fn):
                    return await fn(*args, **kwargs)

                else:
                    return fn(*args, **kwargs)

        return inner

    async def start(self):
        if self.enabled:

            if self._sigmap:
                self._register_signal_handlers()
            
            await self._hide_cursor()
            self._start_time = time.time()
            self._stop_time = None  # Reset value to properly calculate subsequent spinner starts (if any)  # pylint: disable=line-too-long
            self._stop_spin = asyncio.Event()
            self._hide_spin = asyncio.Event()
            try:
                self._spin_thread = asyncio.create_task(self._spin())
            finally:
                # Ensure cursor is not hidden if any failure occurs that prevents
                # getting it back
                await self._show_cursor()

            self.display.start_cli_tasks()

    async def stop(self):
        if self.enabled:
            self._stop_time = time.time()

            if self._dfl_sigmap:
                # Reset registered signal handlers to default ones
                self._reset_signal_handlers()

            if self._spin_thread:
                self._stop_spin.set()
                await self._spin_thread

            await self._clear_line()
            await self.display.stop_cli_tasks()
            await self._show_cursor()

    async def hide(self):
        """Hide the spinner to allow for custom writing to the terminal."""
        thr_is_alive = self._spin_thread and (self._spin_thread.done() is False and self._spin_thread.cancelled() is False)

        if thr_is_alive and not self._hide_spin.is_set():
            
            # set the hidden spinner flag
            self._hide_spin.set()
            await self._clear_line()

            # flush the stdout buffer so the current line
            # can be rewritten to
            await self._loop.run_in_executor(
                None,
                sys.stdout.flush
            )  

    async def show(self):
        """Show the hidden spinner."""
        thr_is_alive = self._spin_thread and (self._spin_thread.done() is False and self._spin_thread.cancelled() is False)

        if thr_is_alive and self._hide_spin.is_set():
            
            # clear the hidden spinner flag
            self._hide_spin.clear()

            # clear the current line so the spinner is not appended to it
            await self._clear_line()      

    async def write(self, text):
        if self.logger_enabled:
            """Write text in the terminal without breaking the spinner."""
            # similar to tqdm.write()
            # https://pypi.python.org/pypi/tqdm#writing-messages
            await self._stdout_lock.acquire()
            await self._clear_line()

            if isinstance(text, (str, bytes)):
                _text = to_unicode(text)
            else:
                _text = str(text)

            # Ensure output is Unicode
            assert isinstance(_text, str)

            await self._loop.run_in_executor(
                None,
                sys.stdout.write,
                
            )

            self._cur_line_len = 0    
            self._stdout_lock.release()  

    async def ok(self, text="OK"):
        if self.enabled:
            await self.display.stop_cli_tasks()
            """Set Ok (success) finalizer to a spinner."""
            _text = text if text else "OK"
            await self._freeze(_text)

    async def fail(self, text="FAIL"):
        if self.enabled:
            await self.display.stop_cli_tasks()
            """Set fail finalizer to a spinner."""
            _text = text if text else "FAIL"
            await self._freeze(_text)

    async def _freeze(self, final_text):
        """Stop spinner, compose last frame and 'freeze' it."""
        text = to_unicode(final_text)
        self._last_frame = self._compose_out(text, mode="last")

        # Should be stopped here, otherwise prints after
        # self._freeze call will mess up the spinner
        await self.stop()
        

        await self._loop.run_in_executor(
            None,
            sys.stdout.write,
            self._last_frame
        )

        self._cur_line_len = 0  

    async def _spin(self):
        while not self._stop_spin.is_set():

            if self._hide_spin.is_set():
                # Wait a bit to avoid wasting cycles
                await asyncio.sleep(self._interval)
                continue
            
            await self._stdout_lock.acquire()
            terminal_size = await self._loop.run_in_executor(
                None,
                get_terminal_size
            )

            terminal_width = terminal_size[0]

            # Compose output
            spin_phase = next(self._cycle)
            out = self._compose_out(spin_phase)

            if len(out) > terminal_width:
                out = f'{out[:terminal_width-1]}...'

            # Write
               
            await self._clear_line()
            
            await self._loop.run_in_executor(
                None,
                sys.stdout.write,
                out
            )

            await self._loop.run_in_executor(
                None,
                sys.stdout.flush
            )

            self._cur_line_len = max(self._cur_line_len, len(out))

            # Wait
            try:
                await asyncio.wait_for(self._stop_spin.wait(), timeout=self._interval)

            except asyncio.TimeoutError:
                pass

            self._stdout_lock.release()

    async def _clear_line(self):
        if sys.stdout.isatty():
            # ANSI Control Sequence EL does not work in Jupyter
            await self._loop.run_in_executor(
                None,
                sys.stdout.write,
                "\r\033[K"
            )

        else:
            fill = " " * self._cur_line_len
            await self._loop.run_in_executor(
                None,
                sys.stdout.write,
                sys.stdout.write,
                f"\r{fill}\r"
            )
            
    @staticmethod
    async def _show_cursor():
        loop = asyncio.get_event_loop()
        if sys.stdout.isatty():
            # ANSI Control Sequence DECTCEM 1 does not work in Jupyter
            await loop.run_in_executor(
                None,
                sys.stdout.write,
                "\033[?25h"
            )

            await loop.run_in_executor(
                None,
                sys.stdout.flush
            )

    @staticmethod
    async def _hide_cursor():
        loop = asyncio.get_event_loop()
        if sys.stdout.isatty():
            # ANSI Control Sequence DECTCEM 1 does not work in Jupyter
            await loop.run_in_executor(
                None,
                sys.stdout.write,
                "\033[?25l"
            )

            await loop.run_in_executor(
                None,
                sys.stdout.flush
            )

    def _register_signal_handlers(self):
        # SIGKILL cannot be caught or ignored, and the receiving
        # process cannot perform any clean-up upon receiving this
        # signal.
        if signal.SIGKILL in self._sigmap:
            raise ValueError(
                "Trying to set handler for SIGKILL signal. "
                "SIGKILL cannot be caught or ignored in POSIX systems."
            )

        for sig, sig_handler in self._sigmap.items():
            # A handler for a particular signal, once set, remains
            # installed until it is explicitly reset. Store default
            # signal handlers for subsequent reset at cleanup phase.
            dfl_handler = signal.getsignal(sig)
            self._dfl_sigmap[sig] = dfl_handler

            # ``signal.SIG_DFL`` and ``signal.SIG_IGN`` are also valid
            # signal handlers and are not callables.
            if callable(sig_handler):
                # ``signal.signal`` accepts handler function which is
                # called with two arguments: signal number and the
                # interrupted stack frame. ``functools.partial`` solves
                # the problem of passing spinner instance into the handler
                # function.
                sig_handler = functools.partial(sig_handler, spinner=self)

            self._loop.add_signal_handler(getattr(signal, sig.name),
                            lambda signame=sig.name: asyncio.create_task(sig_handler(self)))

    def _reset_signal_handlers(self):
        for sig, sig_handler in self._dfl_sigmap.items():
            self._loop.add_signal_handler(getattr(signal, sig.name),
                            lambda signame=sig.name: asyncio.create_task(sig_handler(signame, self)))


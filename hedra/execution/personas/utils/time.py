import datetime
import time
from zebra_automate_logging import Logger

def parse_time(total_time=None):

    logger = Logger()
    session_logger = logger.generate_logger('hedra')

    if total_time is None:
        return None

    if type(total_time) == str:
        date_time = time.strptime(
            total_time,
            "%H:%M:%S"
        )
        time_calculated = datetime.timedelta(
            hours=date_time.tm_hour,
            minutes=date_time.tm_min,
            seconds=date_time.tm_sec
        ).total_seconds()

        return int(time_calculated)

    elif type(total_time) == int:
        return total_time

    elif type(total_time) == float:
        return int(total_time)
        
    else:
        session_logger.error('Error: Could not parse specified execution time. Defaulting to 60 sec.')
        return 60

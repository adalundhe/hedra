import re
from datetime import timedelta

class TimeParser:

    def __init__(self, time_amount: str) -> None:
        self.UNITS = {'s':'seconds', 'm':'minutes', 'h':'hours', 'd':'days', 'w':'weeks'}
        self.time = int(
            timedelta(
                **{
                    self.UNITS.get(
                        m.group(
                            'unit'
                        ).lower(), 
                        'seconds'
                    ): float(m.group('val')
                )
                    for m in re.finditer(
                        r'(?P<val>\d+(\.\d+)?)(?P<unit>[smhdw]?)', 
                        time_amount, 
                        flags=re.I
                    )
                }
            ).total_seconds()
        )

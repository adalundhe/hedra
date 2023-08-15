import re

class MemoryParser:

    def __init__(self, time_amount: str) -> None:
        self.UNITS = {
            'kb':'kilobytes', 
            'mb':'megabytes', 
            'gb':'gigabytes'
        }

        self._conversion_table = {
            'kilobytes': {
                'kilobytes': 1,
                'megabytes': 1/1024,
                'gigabytes': 1/(1024**2)
            },
            'megabytes': {
                'kilobytes': 1024,
                'megabytes': 1,
                'gigabytes': 1/1024
            },
            'gigabytes': {
                'kilobytes': 1024**2,
                'megabytes': 1024,
                'gigabytes': 1
            }
        }
        
        
        parsed_size = {
            self.UNITS.get(
                m.group(
                    'unit'
                ).lower(), 
                'megabytes'
            ): float(m.group('val'))
            for m in re.finditer(
                r'(?P<val>\d+(\.\d+)?)(?P<unit>[smhdw]?)', 
                time_amount, 
                flags=re.I
            )
        }

        self.unit = list(parsed_size.keys()).pop()
        self.size = parsed_size.pop(self.unit)

    def kilobytes(self, accuracy: int = 2):
        conversion_amount = self._conversion_table.get(
            self.unit,
            {}
        ).get(
            'kilobytes',
            1
        )

        return round(
            self.size * conversion_amount,
            accuracy
        )

    def megabytes(self, accuracy: int = 2):
        conversion_amount = self._conversion_table.get(
            self.unit,
            {}
        ).get(
            'megabytes',
            1
        )

        return round(
            self.size * conversion_amount,
            accuracy
        )
    
    def gigabytes(self, accuracy: int = 2):
        conversion_amount = self._conversion_table.get(
            self.unit,
            {}
        ).get(
            'gigabytes',
            1
        )
        

        return round(
            self.size * conversion_amount,
            accuracy
        )
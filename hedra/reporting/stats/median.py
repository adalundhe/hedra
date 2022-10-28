import heapq

class Median:

    __slots__ = (
        'min_heap',
        'max_heap'
    )

    def __init__(self):
        self.min_heap = list()
        self.max_heap = list()

    def update(self, new_value):
        if not self.min_heap:
            heapq.heappush(self.min_heap, new_value)
        elif new_value >= self.min_heap[0]:
            heapq.heappush(self.min_heap, new_value)
        else:
            heapq.heappush(self.max_heap, -1 * new_value)            
            
        if len(self.min_heap) > len(self.max_heap) + 1:
            heapq.heappush(self.max_heap, -1 * heapq.heappop(self.min_heap))
        elif len(self.max_heap) > len(self.min_heap):
            heapq.heappush(self.min_heap, -1 * heapq.heappop(self.max_heap))
            
    def get(self):
        try:
            if len(self.min_heap) == len(self.max_heap):
                return 0.5 * (-1 * self.max_heap[0] + self.min_heap[0])
            return self.min_heap[0] 
        except IndexError:
            return 0

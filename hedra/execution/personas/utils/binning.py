import numpy
from .time import parse_time


def generate_bins(total_actions, batch_size):
    if batch_size == 0:
        batch_size = 1

    num_batches = int(total_actions / batch_size)
    if num_batches < 1:
        num_batches = 1
        
    if total_actions % batch_size > 0:
        num_batches += 1
    
    batch_size = int(total_actions/num_batches)
    remainder = 0

    if total_actions % num_batches > 0:
        remainder = total_actions % num_batches

    bins = []
    for _ in range(num_batches):
        bins.append(batch_size)

    bins[len(bins) - 1] += remainder

    return bins


def calculate_rounded_interval(a, b):
    return round(a / b)


def calculate_total_actions(total_actions=None, total_time=None, batch_size=None, batch_count=None):
    
    if total_actions:
        return total_actions

    elif batch_size and batch_count:
        return batch_size * batch_count

    else:
        total_time = parse_time(total_time)
        return total_time * batch_size


def setup_bins(total_actions=None, total_time=None, batch_size=None):
        bins = []
        if total_time and total_actions:
            batch_size = calculate_rounded_interval(
                total_actions,
                total_time
            )

        bins = generate_bins(
            total_actions,
            batch_size
        )

        return bins
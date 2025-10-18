import cProfile
import pstats
import functools

def profile(output_file="/etc/profile.prof", sort_by="cumulative"):
    """
    Decorator to profile a function with cProfile and save the stats to a file.

    Args:
        output_file (str): Where to save the profile stats.
        sort_by (str): Sort by this key (e.g., 'cumulative', 'time', 'calls').
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            profiler = cProfile.Profile()
            try:
                profiler.enable()
                return func(*args, **kwargs)
            finally:
                profiler.disable()
                with open(output_file, "w+") as f:
                    stats = pstats.Stats(profiler, stream=f)
                    stats.strip_dirs()
                    stats.sort_stats(sort_by)
                    stats.print_stats()
        return wrapper
    return decorator
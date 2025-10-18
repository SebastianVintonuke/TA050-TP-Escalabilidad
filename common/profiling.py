import cProfile
import pstats
import functools
import logging
def profile(output_file="/etc/profiling/profile.prof", sort_by="cumulative"):
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
                logging.info(f"Enabling profiler before running {func}")
                profiler.enable()
                return func(*args, **kwargs)
            except Exception as e:
                logging.info(f"Failed at profiling {e}") 
            finally:
                profiler.disable()
                logging.info(f"Saving profile info to {output_file}")
                profiler.dump_stats(output_file)  # 🔧 Save binary format
                # with open(output_file, "w+") as f:
                #     stats = pstats.Stats(profiler, stream=f)
                #     stats.strip_dirs()
                #     stats.sort_stats(sort_by)
                #     stats.print_stats()
                logging.info(f"Saved profile info to {output_file}")
        return wrapper
    return decorator
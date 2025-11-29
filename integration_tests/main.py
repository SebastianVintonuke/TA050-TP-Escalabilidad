import unittest
import logging
import os

def run_tests_simple(root_folder, verbosity):
    print(f" Discovering tests in : {root_folder}")
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    folder_suite = loader.discover(root_folder)
    suite.addTests(folder_suite)

    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)


def run_tests(root_folder, verbosity):
    print(f"Discovering and running tests from {root_folder}")

    
    # List all subdirectories directly under root_folder
    subfolders = [f.path for f in os.scandir(root_folder) if f.is_dir()]
    
    # Iterate over subfolders and discover tests in each one
    for subfolder in subfolders:
        loader = unittest.TestLoader()
        suite = unittest.TestSuite()
        # Discover tests in each subfolder, matching files like test_*.py
        print(f" Discovering tests in subflder: {subfolder}")
        # fils = os.listdir(subfolder)
        # print(f"SUB GOT{fils}")
        folder_suite = loader.discover(subfolder, pattern="test_*.py")
        suite.addTests(folder_suite)
    
        runner = unittest.TextTestRunner(verbosity=verbosity)
        result = runner.run(suite)
    
    return result
def initialize_log() -> None:
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

def main_tests():
    initialize_log()
    folder = os.getenv("TARGET_FOLDER", "/target_tests")
    verbosity = int(os.getenv("VERBOSITY", "1"))
    run_tests(folder, verbosity)
    # run_tests_simple(folder)

if __name__ == "__main__":
    main_tests()
import unittest
import logging
# Discover and run all tests in the 'my_tests' folder
def run_tests(folder):
    loader = unittest.TestLoader()
    logging.info(f"Discovering and running tests from {folder}")
    suite = loader.discover(folder) 

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result

def main_tests():
	logging.info("Starting tests runner...")
	run_tests("/integration_tests/tests")

if __name__ == "__main__":
	main_tests()
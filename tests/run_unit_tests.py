#!/usr/bin/env python3
"""
Test runner for Akave SDK unit tests.
This script runs all unit tests and provides a summary report.
"""
import unittest
import sys
import os
from io import StringIO

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def discover_and_run_tests():
    """Discover and run all unit tests."""
    # Discover tests in the unit directory
    loader = unittest.TestLoader()
    start_dir = os.path.join(os.path.dirname(__file__), 'unit')
    
    if not os.path.exists(start_dir):
        print(f"Unit tests directory not found: {start_dir}")
        return False
    
    # Discover all test files
    suite = loader.discover(start_dir, pattern='test_*.py')
    
    # Create a test runner with detailed output
    stream = StringIO()
    runner = unittest.TextTestRunner(
        stream=stream,
        verbosity=2,
        buffer=True
    )
    
    # Run the tests
    print("=" * 70)
    print("RUNNING AKAVE SDK UNIT TESTS")
    print("=" * 70)
    
    result = runner.run(suite)
    
    # Print the results
    output = stream.getvalue()
    print(output)
    
    # Print summary
    print("=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    
    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback.split('AssertionError:')[-1].strip() if 'AssertionError:' in traceback else 'See details above'}")
    
    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback.split('Exception:')[-1].strip() if 'Exception:' in traceback else 'See details above'}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    
    if success:
        print("\n✅ ALL TESTS PASSED!")
    else:
        print("\n❌ SOME TESTS FAILED!")
    
    print("=" * 70)
    
    return success


def run_specific_test(test_name):
    """Run a specific test file."""
    test_file = f"test_{test_name}.py"
    test_path = os.path.join(os.path.dirname(__file__), 'unit', test_file)
    
    if not os.path.exists(test_path):
        print(f"Test file not found: {test_path}")
        return False
    
    # Load and run the specific test
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName(f'unit.{test_name}')
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return len(result.failures) == 0 and len(result.errors) == 0


def main():
    """Main entry point for the test runner."""
    if len(sys.argv) > 1:
        # Run specific test
        test_name = sys.argv[1]
        success = run_specific_test(test_name)
    else:
        # Run all tests
        success = discover_and_run_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main() 
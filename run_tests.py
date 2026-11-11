#!/usr/bin/env python3
"""Test runner that works under Python isolated mode (``python3 -I``)."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=os.path.join(os.path.dirname(__file__), "tests"),
                            pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)

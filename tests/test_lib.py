# -*- coding: utf-8 -*-
"""Functions and classes for testing."""

import unittest


class BaseTestCase(unittest.TestCase):
  """The base test case."""

  # Show full diff results.
  maxDiff = None

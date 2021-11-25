# -*- coding: utf-8 -*-
"""Functions and classes for testing."""

import unittest

from acstore.containers import interface as containers_interface


class TestAttributeContainer(containers_interface.AttributeContainer):
  """Attribute container for testing purposes.

  Attributes:
    attribute (str): attribute for testing purposes.
  """

  CONTAINER_TYPE = 'test_container'

  SCHEMA = {'attribute': 'str'}

  def __init__(self):
    """Initializes an attribute container."""
    super(TestAttributeContainer, self).__init__()
    self.attribute = None


class BaseTestCase(unittest.TestCase):
  """The base test case."""

  # Show full diff results.
  maxDiff = None

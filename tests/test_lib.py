# -*- coding: utf-8 -*-
"""Functions and classes for testing."""

import shutil
import tempfile
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


class TempDirectory(object):
  """Class that implements a temporary directory."""

  def __init__(self):
    """Initializes a temporary directory."""
    super(TempDirectory, self).__init__()
    self.name = ''

  def __enter__(self):
    """Make this work with the 'with' statement."""
    self.name = tempfile.mkdtemp()
    return self.name

  def __exit__(self, exception_type, value, traceback):
    """Make this work with the 'with' statement."""
    shutil.rmtree(self.name, True)

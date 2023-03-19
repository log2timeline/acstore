# -*- coding: utf-8 -*-
"""Functions and classes for testing."""

import os
import shutil
import tempfile
import unittest

from acstore.containers import interface as containers_interface


# The path to top of the dfWinReg source tree.
PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# The paths below are all derived from the project path directory.
# They are enumerated explicitly here so that they can be overwritten for
# compatibility with different build systems.
TEST_DATA_PATH = os.path.join(PROJECT_PATH, 'test_data')


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

  def _GetTestFilePath(self, path_segments):
    """Retrieves the path of a test file relative to the test data directory.

    Args:
      path_segments (list[str]): path segments inside the test data directory.

    Returns:
      str: path of the test file.
    """
    # Note that we need to pass the individual path segments to os.path.join
    # and not a list.
    return os.path.join(TEST_DATA_PATH, *path_segments)

  def _SkipIfPathNotExists(self, path):
    """Skips the test if the path does not exist.

    Args:
      path (str): path of a test file.

    Raises:
      SkipTest: if the path does not exist and the test should be skipped.
    """
    if not os.path.exists(path):
      filename = os.path.basename(path)
      raise unittest.SkipTest(f'missing test file: {filename:s}')


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

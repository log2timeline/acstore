#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the attribute container store interface."""

import unittest

from acstore import interface
from acstore.containers import interface as containers_interface
from acstore.containers import manager

from tests import test_lib


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


class AttributeContainerStoreTest(test_lib.BaseTestCase):
  """Tests for the attribute container store interface."""

  # pylint: disable=protected-access

  def testGetAttributeContainerNextSequenceNumber(self):
    """Tests the _GetAttributeContainerNextSequenceNumber function."""
    attribute_container = TestAttributeContainer()

    test_store = interface.AttributeContainerStore()

    sequence_number = test_store._GetAttributeContainerNextSequenceNumber(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(sequence_number, 1)

    sequence_number = test_store._GetAttributeContainerNextSequenceNumber(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(sequence_number, 2)

  def testGetAttributeContainerSchema(self):
    """Tests the _GetAttributeContainerSchema function."""
    attribute_container = TestAttributeContainer()

    test_store = interface.AttributeContainerStore()

    schema = test_store._GetAttributeContainerSchema(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(schema, {})

    manager.AttributeContainersManager.RegisterAttributeContainer(
        TestAttributeContainer)

    try:
      schema = test_store._GetAttributeContainerSchema(
          attribute_container.CONTAINER_TYPE)
      self.assertEqual(schema, TestAttributeContainer.SCHEMA)

    finally:
      manager.AttributeContainersManager.DeregisterAttributeContainer(
          TestAttributeContainer)

  # TODO: add tests for _SetAttributeContainerNextSequenceNumber

  def testSetStorageProfiler(self):
    """Tests the SetStorageProfiler function."""
    test_store = interface.AttributeContainerStore()
    test_store.SetStorageProfiler(None)


if __name__ == '__main__':
  unittest.main()

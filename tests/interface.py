#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the attribute container store interface."""

import unittest

from acstore import interface
from acstore.containers import manager

from tests import test_lib


class AttributeContainerStoreTest(test_lib.BaseTestCase):
  """Tests for the attribute container store interface."""

  # pylint: disable=protected-access

  def testGetAttributeContainerNextSequenceNumber(self):
    """Tests the _GetAttributeContainerNextSequenceNumber function."""
    attribute_container = test_lib.TestAttributeContainer()

    test_store = interface.AttributeContainerStore()

    sequence_number = test_store._GetAttributeContainerNextSequenceNumber(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(sequence_number, 1)

    sequence_number = test_store._GetAttributeContainerNextSequenceNumber(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(sequence_number, 2)

  def testGetAttributeContainerSchema(self):
    """Tests the _GetAttributeContainerSchema function."""
    attribute_container = test_lib.TestAttributeContainer()

    test_store = interface.AttributeContainerStore()

    schema = test_store._GetAttributeContainerSchema(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(schema, {})

    manager.AttributeContainersManager.RegisterAttributeContainer(
        test_lib.TestAttributeContainer)

    try:
      schema = test_store._GetAttributeContainerSchema(
          attribute_container.CONTAINER_TYPE)
      self.assertEqual(schema, test_lib.TestAttributeContainer.SCHEMA)

    finally:
      manager.AttributeContainersManager.DeregisterAttributeContainer(
          test_lib.TestAttributeContainer)

  # TODO: add tests for _SetAttributeContainerNextSequenceNumber

  def testSetStorageProfiler(self):
    """Tests the SetStorageProfiler function."""
    test_store = interface.AttributeContainerStore()
    test_store.SetStorageProfiler(None)


class AttributeContainerStoreWithReadCacheTest(test_lib.BaseTestCase):
  """Tests for the attribute container store with read cache."""

  # pylint: disable=protected-access

  def testCacheAttributeContainerByIndex(self):
    """Tests the _CacheAttributeContainerByIndex function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory():
      test_store = interface.AttributeContainerStoreWithReadCache()

      self.assertEqual(len(test_store._attribute_container_cache), 0)

      test_store._CacheAttributeContainerByIndex(attribute_container, 0)
      self.assertEqual(len(test_store._attribute_container_cache), 1)

  def testGetCachedAttributeContainer(self):
    """Tests the _GetCachedAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory():
      test_store = interface.AttributeContainerStoreWithReadCache()

      cached_container = test_store._GetCachedAttributeContainer(
          attribute_container.CONTAINER_TYPE, 1)
      self.assertIsNone(cached_container)

      test_store._CacheAttributeContainerByIndex(attribute_container, 1)

      cached_container = test_store._GetCachedAttributeContainer(
          attribute_container.CONTAINER_TYPE, 1)
      self.assertIsNotNone(cached_container)


if __name__ == '__main__':
  unittest.main()

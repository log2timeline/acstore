#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the LevelDB-based attribute container store."""

import os
import unittest

try:
  import leveldb
  from acstore import leveldb_store
except ModuleNotFoundError:
  leveldb = None

from acstore.containers import manager as containers_manager

from tests import test_lib


@unittest.skipIf(leveldb is None, 'missing leveldb support')
class LevelDBAttributeContainerStoreTest(test_lib.BaseTestCase):
  """Tests for the LevelDB-based storage file object."""

  # pylint: disable=protected-access

  def setUp(self):
    """Sets up the needed objects used throughout the test."""
    containers_manager.AttributeContainersManager.RegisterAttributeContainer(
        test_lib.TestAttributeContainer)

  def tearDown(self):
    """Cleans up the needed objects used throughout the test."""
    containers_manager.AttributeContainersManager.DeregisterAttributeContainer(
        test_lib.TestAttributeContainer)

  def testGetNumberOfAttributeContainerKeys(self):
    """Tests the _GetNumberOfAttributeContainerKeys function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.leveldb')
      test_store = leveldb_store.LevelDBAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        number_of_containers = test_store._GetNumberOfAttributeContainerKeys(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 0)

        test_store.AddAttributeContainer(attribute_container)

        number_of_containers = test_store._GetNumberOfAttributeContainerKeys(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

        number_of_containers = test_store._GetNumberOfAttributeContainerKeys(
            'bogus')
        self.assertEqual(number_of_containers, 0)

      finally:
        test_store.Close()

  def testRaiseIfNotReadable(self):
    """Tests the _RaiseIfNotReadable function."""
    test_store = leveldb_store.LevelDBAttributeContainerStore()

    with self.assertRaises(IOError):
      test_store._RaiseIfNotReadable()

  def testRaiseIfNotWritable(self):
    """Tests the _RaiseIfNotWritable function."""
    test_store = leveldb_store.LevelDBAttributeContainerStore()

    with self.assertRaises(IOError):
      test_store._RaiseIfNotWritable()

  def testWriteExistingAttributeContainer(self):
    """Tests the _WriteExistingAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.leveldb')
      test_store = leveldb_store.LevelDBAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        number_of_containers = test_store._GetNumberOfAttributeContainerKeys(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 0)

        test_store._WriteNewAttributeContainer(attribute_container)

        number_of_containers = test_store._GetNumberOfAttributeContainerKeys(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

        test_store._WriteExistingAttributeContainer(attribute_container)

        number_of_containers = test_store._GetNumberOfAttributeContainerKeys(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

      finally:
        test_store.Close()

  def testWriteNewAttributeContainer(self):
    """Tests the _WriteNewAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.leveldb')
      test_store = leveldb_store.LevelDBAttributeContainerStore()
      test_store.Open(path=test_path)

      try:
        number_of_containers = test_store._GetNumberOfAttributeContainerKeys(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 0)

        test_store._WriteNewAttributeContainer(attribute_container)

        number_of_containers = test_store._GetNumberOfAttributeContainerKeys(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

      finally:
        test_store.Close()

  def testGetAttributeContainerByIdentifier(self):
    """Tests the GetAttributeContainerByIdentifier function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.leveldb')
      test_store = leveldb_store.LevelDBAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        test_store.AddAttributeContainer(attribute_container)

        identifier = attribute_container.GetIdentifier()

        container = test_store.GetAttributeContainerByIdentifier(
            attribute_container.CONTAINER_TYPE, identifier)
        self.assertIsNotNone(container)

        identifier.sequence_number = 99

        container = test_store.GetAttributeContainerByIdentifier(
            attribute_container.CONTAINER_TYPE, identifier)
        self.assertIsNone(container)

      finally:
        test_store.Close()

  def testGetAttributeContainerByIndex(self):
    """Tests the GetAttributeContainerByIndex function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.leveldb')
      test_store = leveldb_store.LevelDBAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        container = test_store.GetAttributeContainerByIndex(
            attribute_container.CONTAINER_TYPE, 0)
        self.assertIsNone(container)

        test_store.AddAttributeContainer(attribute_container)

        container = test_store.GetAttributeContainerByIndex(
            attribute_container.CONTAINER_TYPE, 0)
        self.assertIsNotNone(container)

        container = test_store.GetAttributeContainerByIndex('bogus', 0)
        self.assertIsNone(container)

      finally:
        test_store.Close()

  def testGetAttributeContainers(self):
    """Tests the GetAttributeContainers function."""
    attribute_container = test_lib.TestAttributeContainer()
    attribute_container.attribute = '8f0bf95a7959baad9666b21a7feed79d'

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.leveldb')
      test_store = leveldb_store.LevelDBAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        containers = list(test_store.GetAttributeContainers(
            attribute_container.CONTAINER_TYPE))
        self.assertEqual(len(containers), 0)

        test_store.AddAttributeContainer(attribute_container)

        containers = list(test_store.GetAttributeContainers(
            attribute_container.CONTAINER_TYPE))
        self.assertEqual(len(containers), 1)

        filter_expression = 'attribute == "8f0bf95a7959baad9666b21a7feed79d"'
        containers = list(test_store.GetAttributeContainers(
            attribute_container.CONTAINER_TYPE,
            filter_expression=filter_expression))
        self.assertEqual(len(containers), 1)

        filter_expression = 'attribute != "8f0bf95a7959baad9666b21a7feed79d"'
        containers = list(test_store.GetAttributeContainers(
            attribute_container.CONTAINER_TYPE,
            filter_expression=filter_expression))
        self.assertEqual(len(containers), 0)

      finally:
        test_store.Close()

  def testGetNumberOfAttributeContainers(self):
    """Tests the GetNumberOfAttributeContainers function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.leveldb')
      test_store = leveldb_store.LevelDBAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 0)

        test_store.AddAttributeContainer(attribute_container)

        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

        number_of_containers = test_store.GetNumberOfAttributeContainers(
            'bogus')
        self.assertEqual(number_of_containers, 0)

      finally:
        test_store.Close()

  def testHasAttributeContainers(self):
    """Tests the HasAttributeContainers function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.leveldb')
      test_store = leveldb_store.LevelDBAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        result = test_store.HasAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertFalse(result)

        test_store.AddAttributeContainer(attribute_container)

        result = test_store.HasAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertTrue(result)

        result = test_store.HasAttributeContainers('bogus')
        self.assertFalse(result)

      finally:
        test_store.Close()

  # TODO: add tests for Open and Close


if __name__ == '__main__':
  unittest.main()

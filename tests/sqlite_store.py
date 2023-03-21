#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the SQLite-based attribute container store."""

import os
import unittest

from acstore import sqlite_store
from acstore.containers import manager as containers_manager

from tests import test_lib


class _TestSQLiteAttributeContainerStoreV20220716(
    sqlite_store.SQLiteAttributeContainerStore):
  """Test class for testing format compatibility checks."""

  _FORMAT_VERSION = 20220716
  _APPEND_COMPATIBLE_FORMAT_VERSION = 20211121
  _UPGRADE_COMPATIBLE_FORMAT_VERSION = 20211121
  _READ_COMPATIBLE_FORMAT_VERSION = 20211121


class _TestSQLiteAttributeContainerStoreV20221023(
    sqlite_store.SQLiteAttributeContainerStore):
  """Test class for testing format compatibility checks."""

  _FORMAT_VERSION = 20221023
  _APPEND_COMPATIBLE_FORMAT_VERSION = 20221023
  _UPGRADE_COMPATIBLE_FORMAT_VERSION = 20221023
  _READ_COMPATIBLE_FORMAT_VERSION = 20211121


# TODO add tests for PythonAST2SQL.


class SQLiteSchemaHelperTest(test_lib.BaseTestCase):
  """Tests for the SQLite schema helper."""

  # pylint: disable=protected-access

  def testGetStorageDataType(self):
    """Tests the GetStorageDataType function."""
    schema_helper = sqlite_store.SQLiteSchemaHelper()

    data_type = schema_helper.GetStorageDataType('bool')
    self.assertEqual(data_type, 'INTEGER')

    data_type = schema_helper.GetStorageDataType('int')
    self.assertEqual(data_type, 'INTEGER')

    data_type = schema_helper.GetStorageDataType('str')
    self.assertEqual(data_type, 'TEXT')

    data_type = schema_helper.GetStorageDataType('timestamp')
    self.assertEqual(data_type, 'BIGINT')

    data_type = schema_helper.GetStorageDataType('AttributeContainerIdentifier')
    self.assertEqual(data_type, 'TEXT')

  def testDeserializeValue(self):
    """Tests the DeserializeValue function."""
    schema_helper = sqlite_store.SQLiteSchemaHelper()

    value = schema_helper.DeserializeValue('bool', 0)
    self.assertFalse(value)

    value = schema_helper.DeserializeValue('bool', 1)
    self.assertTrue(value)

    value = schema_helper.DeserializeValue('int', 1)
    self.assertEqual(value, 1)

    value = schema_helper.DeserializeValue('str', 'one')
    self.assertEqual(value, 'one')

    value = schema_helper.DeserializeValue('timestamp', 1)
    self.assertEqual(value, 1)

    # TODO: add test for AttributeContainerIdentifier

  def testSerializeValue(self):
    """Tests the SerializeValue function."""
    schema_helper = sqlite_store.SQLiteSchemaHelper()

    value = schema_helper.SerializeValue('bool', False)
    self.assertEqual(value, 0)

    value = schema_helper.SerializeValue('bool', True)
    self.assertEqual(value, 1)

    value = schema_helper.SerializeValue('int', 1)
    self.assertEqual(value, 1)

    value = schema_helper.SerializeValue('str', 'one')
    self.assertEqual(value, 'one')

    value = schema_helper.SerializeValue('timestamp', 1)
    self.assertEqual(value, 1)

    # TODO: add test for AttributeContainerIdentifier


class SQLiteAttributeContainerStoreTest(test_lib.BaseTestCase):
  """Tests for the SQLite-based storage file object."""

  # pylint: disable=protected-access

  def setUp(self):
    """Sets up the needed objects used throughout the test."""
    containers_manager.AttributeContainersManager.RegisterAttributeContainer(
        test_lib.TestAttributeContainer)

  def tearDown(self):
    """Cleans up the needed objects used throughout the test."""
    containers_manager.AttributeContainersManager.DeregisterAttributeContainer(
        test_lib.TestAttributeContainer)

  def testCacheAttributeContainerByIndex(self):
    """Tests the _CacheAttributeContainerByIndex function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory():
      test_store = sqlite_store.SQLiteAttributeContainerStore()

      self.assertEqual(len(test_store._attribute_container_cache), 0)

      test_store._CacheAttributeContainerByIndex(attribute_container, 0)
      self.assertEqual(len(test_store._attribute_container_cache), 1)

  def testCheckStorageMetadata(self):
    """Tests the _CheckStorageMetadata function."""
    with test_lib.TempDirectory():
      test_store = sqlite_store.SQLiteAttributeContainerStore()

      metadata_values = {
          'format_version': f'{test_store._FORMAT_VERSION:d}',
          'serialization_format': 'json'}
      test_store._CheckStorageMetadata(metadata_values)

      metadata_values['format_version'] = 'bogus'
      with self.assertRaises(IOError):
        test_store._CheckStorageMetadata(metadata_values)

      metadata_values['format_version'] = '1'
      with self.assertRaises(IOError):
        test_store._CheckStorageMetadata(metadata_values)

      metadata_values['format_version'] = f'{test_store._FORMAT_VERSION:d}'
      metadata_values['serialization_format'] = 'bogus'
      with self.assertRaises(IOError):
        test_store._CheckStorageMetadata(metadata_values)

      metadata_values['serialization_format'] = 'json'

  def testCreateAttributeContainerTable(self):
    """Tests the _CreateAttributeContainerTable function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        test_store._CreateAttributeContainerTable(
            attribute_container.CONTAINER_TYPE)

        with self.assertRaises(IOError):
          test_store._CreateAttributeContainerTable(
              attribute_container.CONTAINER_TYPE)

      finally:
        test_store.Close()

  # TODO: add tests for _CreatetAttributeContainerFromRow
  # TODO: add tests for _Flush
  # TODO: add tests for _FlushWriteCache

  def testGetAttributeContainersWithFilter(self):
    """Tests the _GetAttributeContainersWithFilter function."""
    attribute_container = test_lib.TestAttributeContainer()
    attribute_container.attribute = '8f0bf95a7959baad9666b21a7feed79d'

    column_names = ['attribute']

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        containers = list(test_store._GetAttributeContainersWithFilter(
            attribute_container.CONTAINER_TYPE, column_names=column_names))
        self.assertEqual(len(containers), 0)

        test_store.AddAttributeContainer(attribute_container)

        containers = list(test_store._GetAttributeContainersWithFilter(
            attribute_container.CONTAINER_TYPE, column_names=column_names))
        self.assertEqual(len(containers), 1)

        filter_expression = 'attribute == "8f0bf95a7959baad9666b21a7feed79d"'
        containers = list(test_store._GetAttributeContainersWithFilter(
            attribute_container.CONTAINER_TYPE, column_names=column_names,
            filter_expression=filter_expression))
        self.assertEqual(len(containers), 1)

        filter_expression = 'attribute != "8f0bf95a7959baad9666b21a7feed79d"'
        containers = list(test_store._GetAttributeContainersWithFilter(
            attribute_container.CONTAINER_TYPE, column_names=column_names,
            filter_expression=filter_expression))
        self.assertEqual(len(containers), 0)

        containers = list(test_store._GetAttributeContainersWithFilter(
            'bogus', column_names=column_names))
        self.assertEqual(len(containers), 0)

      finally:
        test_store.Close()

  def testGetCachedAttributeContainer(self):
    """Tests the _GetCachedAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory():
      test_store = sqlite_store.SQLiteAttributeContainerStore()

      cached_container = test_store._GetCachedAttributeContainer(
          attribute_container.CONTAINER_TYPE, 1)
      self.assertIsNone(cached_container)

      test_store._CacheAttributeContainerByIndex(attribute_container, 1)

      cached_container = test_store._GetCachedAttributeContainer(
          attribute_container.CONTAINER_TYPE, 1)
      self.assertIsNotNone(cached_container)

  def testHasTable(self):
    """Tests the _HasTable function."""
    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        test_store._CreateAttributeContainerTable('test_container')

        result = test_store._HasTable('test_container')
        self.assertTrue(result)

        result = test_store._HasTable('bogus')
        self.assertFalse(result)

      finally:
        test_store.Close()

  def testRaiseIfNotReadable(self):
    """Tests the _RaiseIfNotReadable function."""
    test_store = sqlite_store.SQLiteAttributeContainerStore()

    with self.assertRaises(IOError):
      test_store._RaiseIfNotReadable()

  def testRaiseIfNotWritable(self):
    """Tests the _RaiseIfNotWritable function."""
    test_store = sqlite_store.SQLiteAttributeContainerStore()

    with self.assertRaises(IOError):
      test_store._RaiseIfNotWritable()

  # TODO: add tests for _ReadAndCheckStorageMetadata
  # TODO: add tests for _ReadMetadata
  # TODO: add tests for _UpdateStorageMetadataFormatVersion

  def testWriteExistingAttributeContainer(self):
    """Tests the _WriteExistingAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 0)

        test_store._WriteNewAttributeContainer(attribute_container)

        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

        test_store._WriteExistingAttributeContainer(attribute_container)

        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

      finally:
        test_store.Close()

  # TODO: add tests for _WriteMetadata
  # TODO: add tests for _WriteMetadataValue

  def testWriteNewAttributeContainer(self):
    """Tests the _WriteNewAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 0)

        test_store._WriteNewAttributeContainer(attribute_container)

        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

      finally:
        test_store.Close()

  def testAddAttributeContainer(self):
    """Tests the AddAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 0)

        test_store.AddAttributeContainer(attribute_container)

        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

      finally:
        test_store.Close()

      with self.assertRaises(IOError):
        test_store.AddAttributeContainer(attribute_container)

  # TODO: add tests for CheckSupportedFormat

  def testGetAttributeContainerByIdentifier(self):
    """Tests the GetAttributeContainerByIdentifier function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
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
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
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
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
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

        with self.assertRaises(IOError):
          list(test_store.GetAttributeContainers('bogus'))

      finally:
        test_store.Close()

  def testGetNumberOfAttributeContainers(self):
    """Tests the GetNumberOfAttributeContainers function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 0)

        test_store.AddAttributeContainer(attribute_container)

        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

        # Test for a supported container type that does not have a table
        # present in the storage file.
        query = f'DROP TABLE {attribute_container.CONTAINER_TYPE:s}'
        test_store._cursor.execute(query)
        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 0)

      finally:
        test_store.Close()

  def testHasAttributeContainers(self):
    """Tests the HasAttributeContainers function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
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

  def testUpdateAttributeContainer(self):
    """Tests the UpdateAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    with test_lib.TempDirectory() as temp_directory:
      test_path = os.path.join(temp_directory, 'acstore.sqlite')
      test_store = sqlite_store.SQLiteAttributeContainerStore()
      test_store.Open(path=test_path, read_only=False)

      try:
        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 0)

        test_store.AddAttributeContainer(attribute_container)

        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

        test_store.UpdateAttributeContainer(attribute_container)

        number_of_containers = test_store.GetNumberOfAttributeContainers(
            attribute_container.CONTAINER_TYPE)
        self.assertEqual(number_of_containers, 1)

      finally:
        test_store.Close()

  def testVersionCompatibility(self):
    """Tests the version compatibility methods."""
    with test_lib.TempDirectory() as temp_directory:
      v1_storage_path = os.path.join(temp_directory, 'v20220716.sqlite')
      v1_test_store = _TestSQLiteAttributeContainerStoreV20220716()
      v1_test_store.Open(path=v1_storage_path, read_only=False)
      v1_test_store.Close()

      v2_test_store_rw = _TestSQLiteAttributeContainerStoreV20221023()

      with self.assertRaises((IOError, OSError)):
        v2_test_store_rw.Open(path=v1_storage_path, read_only=False)

      v2_test_store_ro = _TestSQLiteAttributeContainerStoreV20221023()
      v2_test_store_ro.Open(path=v1_storage_path, read_only=True)
      v2_test_store_ro.Close()


if __name__ == '__main__':
  unittest.main()

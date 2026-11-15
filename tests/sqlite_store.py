#!/usr/bin/env python3
"""Tests for the SQLite-based attribute container store."""

import json
import os
import sqlite3
import unittest

from acstore import interface
from acstore import sqlite_store
from acstore.containers import interface as containers_interface
from acstore.containers import manager as containers_manager
from acstore.helpers import schema

from tests import test_lib


class _TestSQLiteAttributeContainerStoreV20220716(
    sqlite_store.SQLiteAttributeContainerStore
):
    """Test class for testing format compatibility checks."""

    _FORMAT_VERSION = 20220716
    _APPEND_COMPATIBLE_FORMAT_VERSION = 20211121
    _UPGRADE_COMPATIBLE_FORMAT_VERSION = 20211121
    _READ_COMPATIBLE_FORMAT_VERSION = 20211121


class _TestSQLiteAttributeContainerStoreV20221023(
    sqlite_store.SQLiteAttributeContainerStore
):
    """Test class for testing format compatibility checks."""

    _FORMAT_VERSION = 20221023
    _APPEND_COMPATIBLE_FORMAT_VERSION = 20221023
    _UPGRADE_COMPATIBLE_FORMAT_VERSION = 20221023
    _READ_COMPATIBLE_FORMAT_VERSION = 20211121


class _TestReferenceAttributeContainer(containers_interface.AttributeContainer):
    """Attribute container with a reference for testing purposes.

    Attributes:
      reference (AttributeContainerIdentifier): identifier of a test container.
    """

    CONTAINER_TYPE = "test_reference"

    SCHEMA = {"reference": "test.Reference"}

    def __init__(self):
        """Initializes an attribute container."""
        super().__init__()
        self.reference = None


class _TestJSONReferenceAttributeSerializer(interface.AttributeSerializer):
    """JSON reference attribute serializer for testing purposes."""

    def DeserializeValue(self, value):
        """Deserializes a value.

        Args:
          value (str): serialized value.

        Returns:
          AttributeContainerIdentifier: runtime value.
        """
        identifier = containers_interface.AttributeContainerIdentifier()
        identifier.CopyFromString(value)
        return identifier

    def SerializeValue(self, value):
        """Serializes a value.

        Args:
          value (AttributeContainerIdentifier): runtime value.

        Returns:
          str: serialized value.
        """
        return value.CopyToString()


class _TestSQLiteReferenceAttributeSerializer(sqlite_store.SQLiteAttributeSerializer):
    """SQLite reference attribute serializer for testing purposes."""

    STORAGE_DATA_TYPE = "INTEGER"

    def DeserializeValue(self, value):
        """Deserializes a value.

        Args:
          value (int|str): serialized value, where a string is either JSON
              or an integer that was stored in a TEXT column.

        Returns:
          AttributeContainerIdentifier: runtime value.
        """
        if isinstance(value, str) and value.startswith('"'):
            identifier = containers_interface.AttributeContainerIdentifier()
            identifier.CopyFromString(json.loads(value))
            return identifier

        return containers_interface.AttributeContainerIdentifier(
            name="test_container", sequence_number=int(value)
        )

    def SerializeValue(self, value):
        """Serializes a value.

        Args:
          value (AttributeContainerIdentifier): runtime value.

        Returns:
          int: serialized value.
        """
        return value.sequence_number


# TODO add tests for PythonAST2SQL.


class SQLiteSchemaHelperTest(test_lib.BaseTestCase):
    """Tests for the SQLite schema helper."""

    # pylint: disable=protected-access

    def testGetStorageDataType(self):
        """Tests the GetStorageDataType function."""
        schema_helper = sqlite_store.SQLiteSchemaHelper()

        data_type = schema_helper.GetStorageDataType("bool")
        self.assertEqual(data_type, "INTEGER")

        data_type = schema_helper.GetStorageDataType("int")
        self.assertEqual(data_type, "INTEGER")

        data_type = schema_helper.GetStorageDataType("str")
        self.assertEqual(data_type, "TEXT")

        data_type = schema_helper.GetStorageDataType("timestamp")
        self.assertEqual(data_type, "BIGINT")

        data_type = schema_helper.GetStorageDataType("AttributeContainerIdentifier")
        self.assertEqual(data_type, "TEXT")

    def testDeserializeValue(self):
        """Tests the DeserializeValue function."""
        schema_helper = sqlite_store.SQLiteSchemaHelper()

        value = schema_helper.DeserializeValue("bool", 0)
        self.assertFalse(value)

        value = schema_helper.DeserializeValue("bool", 1)
        self.assertTrue(value)

        value = schema_helper.DeserializeValue("int", 1)
        self.assertEqual(value, 1)

        value = schema_helper.DeserializeValue("str", "one")
        self.assertEqual(value, "one")

        value = schema_helper.DeserializeValue("timestamp", 1)
        self.assertEqual(value, 1)

        # TODO: add test for AttributeContainerIdentifier

    def testSerializeValue(self):
        """Tests the SerializeValue function."""
        schema_helper = sqlite_store.SQLiteSchemaHelper()

        value = schema_helper.SerializeValue("bool", False)
        self.assertEqual(value, 0)

        value = schema_helper.SerializeValue("bool", True)
        self.assertEqual(value, 1)

        value = schema_helper.SerializeValue("int", 1)
        self.assertEqual(value, 1)

        value = schema_helper.SerializeValue("str", "one")
        self.assertEqual(value, "one")

        value = schema_helper.SerializeValue("timestamp", 1)
        self.assertEqual(value, 1)

        # TODO: add test for AttributeContainerIdentifier


class SQLiteSchemaHelperWithAttributeSerializerTest(test_lib.BaseTestCase):
    """Tests for the SQLite schema helper with attribute serializers."""

    def setUp(self):
        """Sets up the needed objects used throughout the test."""
        schema.SchemaHelper.RegisterDataType(
            "test.Reference",
            {
                "json": _TestJSONReferenceAttributeSerializer(),
                "sqlite": _TestSQLiteReferenceAttributeSerializer(),
            },
        )

    def tearDown(self):
        """Cleans up the needed objects used throughout the test."""
        schema.SchemaHelper.DeregisterDataType("test.Reference")

    def testGetStorageDataType(self):
        """Tests the GetStorageDataType function."""
        test_helper = sqlite_store.SQLiteSchemaHelper()

        data_type = test_helper.GetStorageDataType("test.Reference")
        self.assertEqual(data_type, "INTEGER")

    def testDeserializeValue(self):
        """Tests the DeserializeValue function."""
        test_helper = sqlite_store.SQLiteSchemaHelper()

        value = test_helper.DeserializeValue("test.Reference", 5)
        self.assertEqual(value.CopyToString(), "test_container.5")

        value = test_helper.DeserializeValue("test.Reference", '"test_container.5"')
        self.assertEqual(value.CopyToString(), "test_container.5")

        value = test_helper.DeserializeValue("test.Reference", None)
        self.assertIsNone(value)

    def testSerializeValue(self):
        """Tests the SerializeValue function."""
        test_helper = sqlite_store.SQLiteSchemaHelper()

        identifier = containers_interface.AttributeContainerIdentifier(
            name="test_container", sequence_number=5
        )
        value = test_helper.SerializeValue("test.Reference", identifier)
        self.assertEqual(value, 5)

        value = test_helper.SerializeValue("test.Reference", None)
        self.assertIsNone(value)


class SQLiteAttributeContainerStoreTest(test_lib.BaseTestCase):
    """Tests for the SQLite-based storage file object."""

    # pylint: disable=protected-access

    def setUp(self):
        """Sets up the needed objects used throughout the test."""
        containers_manager.AttributeContainersManager.RegisterAttributeContainer(
            test_lib.TestAttributeContainer
        )

    def tearDown(self):
        """Cleans up the needed objects used throughout the test."""
        containers_manager.AttributeContainersManager.DeregisterAttributeContainer(
            test_lib.TestAttributeContainer
        )

    def testCheckStorageMetadata(self):
        """Tests the _CheckStorageMetadata function."""
        with test_lib.TempDirectory():
            test_store = sqlite_store.SQLiteAttributeContainerStore()

            metadata_values = {
                "format_version": f"{test_store._FORMAT_VERSION:d}",
                "serialization_format": "json",
            }
            test_store._CheckStorageMetadata(metadata_values)

            metadata_values["format_version"] = "bogus"
            with self.assertRaises(OSError):
                test_store._CheckStorageMetadata(metadata_values)

            metadata_values["format_version"] = "1"
            with self.assertRaises(OSError):
                test_store._CheckStorageMetadata(metadata_values)

            metadata_values["format_version"] = f"{test_store._FORMAT_VERSION:d}"
            metadata_values["serialization_format"] = "bogus"
            with self.assertRaises(OSError):
                test_store._CheckStorageMetadata(metadata_values)

            metadata_values["serialization_format"] = "json"

    def testCreateAttributeContainerTable(self):
        """Tests the _CreateAttributeContainerTable function."""
        attribute_container = test_lib.TestAttributeContainer()

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                test_store._CreateAttributeContainerTable(
                    attribute_container.CONTAINER_TYPE
                )

                with self.assertRaises(OSError):
                    test_store._CreateAttributeContainerTable(
                        attribute_container.CONTAINER_TYPE
                    )

            finally:
                test_store.Close()

    # TODO: add tests for _CreateAttributeContainerFromRow
    # TODO: add tests for _Flush
    # TODO: add tests for _FlushWriteCache

    def testGetAttributeContainersWithFilter(self):
        """Tests the _GetAttributeContainersWithFilter function."""
        attribute_container = test_lib.TestAttributeContainer()
        attribute_container.attribute = "8f0bf95a7959baad9666b21a7feed79d"

        column_names = ["attribute"]

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                containers = list(
                    test_store._GetAttributeContainersWithFilter(
                        attribute_container.CONTAINER_TYPE, column_names=column_names
                    )
                )
                self.assertEqual(len(containers), 0)

                test_store.AddAttributeContainer(attribute_container)

                containers = list(
                    test_store._GetAttributeContainersWithFilter(
                        attribute_container.CONTAINER_TYPE, column_names=column_names
                    )
                )
                self.assertEqual(len(containers), 1)

                filter_expression = 'attribute == "8f0bf95a7959baad9666b21a7feed79d"'
                containers = list(
                    test_store._GetAttributeContainersWithFilter(
                        attribute_container.CONTAINER_TYPE,
                        column_names=column_names,
                        filter_expression=filter_expression,
                    )
                )
                self.assertEqual(len(containers), 1)

                filter_expression = 'attribute != "8f0bf95a7959baad9666b21a7feed79d"'
                containers = list(
                    test_store._GetAttributeContainersWithFilter(
                        attribute_container.CONTAINER_TYPE,
                        column_names=column_names,
                        filter_expression=filter_expression,
                    )
                )
                self.assertEqual(len(containers), 0)

                containers = list(
                    test_store._GetAttributeContainersWithFilter(
                        "bogus", column_names=column_names
                    )
                )
                self.assertEqual(len(containers), 0)

            finally:
                test_store.Close()

    def testGetNumberOfAttributeContainerRows(self):
        """Tests the _GetNumberOfAttributeContainerRows function."""
        attribute_container = test_lib.TestAttributeContainer()

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                number_of_containers = test_store._GetNumberOfAttributeContainerRows(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 0)

                test_store.AddAttributeContainer(attribute_container)

                number_of_containers = test_store._GetNumberOfAttributeContainerRows(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 1)

                # Test for a supported container type that does not have a table
                # present in the storage file.
                query = f"DROP TABLE {attribute_container.CONTAINER_TYPE:s}"
                test_store._cursor.execute(query)
                number_of_containers = test_store._GetNumberOfAttributeContainerRows(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 0)

            finally:
                test_store.Close()

    def testHasTable(self):
        """Tests the _HasTable function."""
        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                test_store._CreateAttributeContainerTable("test_container")

                result = test_store._HasTable("test_container")
                self.assertTrue(result)

                result = test_store._HasTable("bogus")
                self.assertFalse(result)

            finally:
                test_store.Close()

    def testRaiseIfNotReadable(self):
        """Tests the _RaiseIfNotReadable function."""
        test_store = sqlite_store.SQLiteAttributeContainerStore()

        with self.assertRaises(OSError):
            test_store._RaiseIfNotReadable()

    def testRaiseIfNotWritable(self):
        """Tests the _RaiseIfNotWritable function."""
        test_store = sqlite_store.SQLiteAttributeContainerStore()

        with self.assertRaises(OSError):
            test_store._RaiseIfNotWritable()

    # TODO: add tests for _ReadAndCheckStorageMetadata
    # TODO: add tests for _ReadMetadata
    # TODO: add tests for _UpdateStorageMetadataFormatVersion

    def testWriteExistingAttributeContainer(self):
        """Tests the _WriteExistingAttributeContainer function."""
        attribute_container = test_lib.TestAttributeContainer()

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                number_of_containers = test_store._GetNumberOfAttributeContainerRows(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 0)

                test_store._WriteNewAttributeContainer(attribute_container)

                number_of_containers = test_store._GetNumberOfAttributeContainerRows(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 1)

                test_store._WriteExistingAttributeContainer(attribute_container)

                number_of_containers = test_store._GetNumberOfAttributeContainerRows(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 1)

            finally:
                test_store.Close()

    # TODO: add tests for _WriteMetadata
    # TODO: add tests for _WriteMetadataValue

    def testWriteNewAttributeContainer(self):
        """Tests the _WriteNewAttributeContainer function."""
        attribute_container = test_lib.TestAttributeContainer()

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                number_of_containers = test_store._GetNumberOfAttributeContainerRows(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 0)

                test_store._WriteNewAttributeContainer(attribute_container)

                number_of_containers = test_store._GetNumberOfAttributeContainerRows(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 1)

            finally:
                test_store.Close()

    def testAddAttributeContainer(self):
        """Tests the AddAttributeContainer function."""
        attribute_container = test_lib.TestAttributeContainer()

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                number_of_containers = test_store.GetNumberOfAttributeContainers(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 0)

                test_store.AddAttributeContainer(attribute_container)

                number_of_containers = test_store.GetNumberOfAttributeContainers(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 1)

            finally:
                test_store.Close()

            with self.assertRaises(OSError):
                test_store.AddAttributeContainer(attribute_container)

    # TODO: add tests for CheckSupportedFormat

    def testGetAttributeContainerByIdentifier(self):
        """Tests the GetAttributeContainerByIdentifier function."""
        attribute_container = test_lib.TestAttributeContainer()

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                test_store.AddAttributeContainer(attribute_container)

                identifier = attribute_container.GetIdentifier()

                container = test_store.GetAttributeContainerByIdentifier(
                    attribute_container.CONTAINER_TYPE, identifier
                )
                self.assertIsNotNone(container)

                identifier.sequence_number = 99

                container = test_store.GetAttributeContainerByIdentifier(
                    attribute_container.CONTAINER_TYPE, identifier
                )
                self.assertIsNone(container)

            finally:
                test_store.Close()

    def testGetAttributeContainerByIndex(self):
        """Tests the GetAttributeContainerByIndex function."""
        attribute_container = test_lib.TestAttributeContainer()

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                container = test_store.GetAttributeContainerByIndex(
                    attribute_container.CONTAINER_TYPE, 0
                )
                self.assertIsNone(container)

                test_store.AddAttributeContainer(attribute_container)

                container = test_store.GetAttributeContainerByIndex(
                    attribute_container.CONTAINER_TYPE, 0
                )
                self.assertIsNotNone(container)

                container = test_store.GetAttributeContainerByIndex("bogus", 0)
                self.assertIsNone(container)

            finally:
                test_store.Close()

    def testGetAttributeContainers(self):
        """Tests the GetAttributeContainers function."""
        attribute_container = test_lib.TestAttributeContainer()
        attribute_container.attribute = "8f0bf95a7959baad9666b21a7feed79d"

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                containers = list(
                    test_store.GetAttributeContainers(
                        attribute_container.CONTAINER_TYPE
                    )
                )
                self.assertEqual(len(containers), 0)

                test_store.AddAttributeContainer(attribute_container)

                containers = list(
                    test_store.GetAttributeContainers(
                        attribute_container.CONTAINER_TYPE
                    )
                )
                self.assertEqual(len(containers), 1)

                filter_expression = 'attribute == "8f0bf95a7959baad9666b21a7feed79d"'
                containers = list(
                    test_store.GetAttributeContainers(
                        attribute_container.CONTAINER_TYPE,
                        filter_expression=filter_expression,
                    )
                )
                self.assertEqual(len(containers), 1)

                filter_expression = 'attribute != "8f0bf95a7959baad9666b21a7feed79d"'
                containers = list(
                    test_store.GetAttributeContainers(
                        attribute_container.CONTAINER_TYPE,
                        filter_expression=filter_expression,
                    )
                )
                self.assertEqual(len(containers), 0)

                with self.assertRaises(OSError):
                    list(test_store.GetAttributeContainers("bogus"))

            finally:
                test_store.Close()

    def testGetNumberOfAttributeContainers(self):
        """Tests the GetNumberOfAttributeContainers function."""
        attribute_container = test_lib.TestAttributeContainer()

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                number_of_containers = test_store.GetNumberOfAttributeContainers(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 0)

                test_store.AddAttributeContainer(attribute_container)

                number_of_containers = test_store.GetNumberOfAttributeContainers(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 1)

                number_of_containers = test_store.GetNumberOfAttributeContainers(
                    "bogus"
                )
                self.assertEqual(number_of_containers, 0)

            finally:
                test_store.Close()

    def testHasAttributeContainers(self):
        """Tests the HasAttributeContainers function."""
        attribute_container = test_lib.TestAttributeContainer()

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                result = test_store.HasAttributeContainers(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertFalse(result)

                test_store.AddAttributeContainer(attribute_container)

                result = test_store.HasAttributeContainers(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertTrue(result)

                result = test_store.HasAttributeContainers("bogus")
                self.assertFalse(result)

            finally:
                test_store.Close()

    # TODO: add tests for Open and Close

    def testUpdateAttributeContainer(self):
        """Tests the UpdateAttributeContainer function."""
        attribute_container = test_lib.TestAttributeContainer()

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                number_of_containers = test_store.GetNumberOfAttributeContainers(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 0)

                test_store.AddAttributeContainer(attribute_container)

                number_of_containers = test_store.GetNumberOfAttributeContainers(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 1)

                test_store.UpdateAttributeContainer(attribute_container)

                number_of_containers = test_store.GetNumberOfAttributeContainers(
                    attribute_container.CONTAINER_TYPE
                )
                self.assertEqual(number_of_containers, 1)

            finally:
                test_store.Close()

    def testVersionCompatibility(self):
        """Tests the version compatibility methods."""
        with test_lib.TempDirectory() as temp_directory:
            v1_storage_path = os.path.join(temp_directory, "v20220716.sqlite")
            v1_test_store = _TestSQLiteAttributeContainerStoreV20220716()
            v1_test_store.Open(path=v1_storage_path, read_only=False)
            v1_test_store.Close()

            v2_test_store_rw = _TestSQLiteAttributeContainerStoreV20221023()

            with self.assertRaises(OSError):
                v2_test_store_rw.Open(path=v1_storage_path, read_only=False)

            v2_test_store_ro = _TestSQLiteAttributeContainerStoreV20221023()
            v2_test_store_ro.Open(path=v1_storage_path, read_only=True)
            v2_test_store_ro.Close()


class SQLiteAttributeContainerStoreWithAttributeSerializerTest(test_lib.BaseTestCase):
    """Tests for the SQLite-based store with attribute serializers."""

    def setUp(self):
        """Sets up the needed objects used throughout the test."""
        containers_manager.AttributeContainersManager.RegisterAttributeContainer(
            _TestReferenceAttributeContainer
        )
        schema.SchemaHelper.RegisterDataType(
            "test.Reference", {"json": _TestJSONReferenceAttributeSerializer()}
        )

    def tearDown(self):
        """Cleans up the needed objects used throughout the test."""
        schema.SchemaHelper.DeregisterDataType("test.Reference")
        containers_manager.AttributeContainersManager.DeregisterAttributeContainer(
            _TestReferenceAttributeContainer
        )

    def _CreateTestContainer(self, sequence_number):
        """Creates a test attribute container.

        Args:
          sequence_number (int): sequence number of the referenced container.

        Returns:
          _TestReferenceAttributeContainer: attribute container.
        """
        attribute_container = _TestReferenceAttributeContainer()
        attribute_container.reference = (
            containers_interface.AttributeContainerIdentifier(
                name="test_container", sequence_number=sequence_number
            )
        )
        return attribute_container

    def _GetColumnValues(self, path):
        """Retrieves the type and value of the reference column of every row.

        Args:
          path (str): path of the attribute container store.

        Returns:
          list[tuple[str, object]]: SQLite type and value of the reference column.
        """
        connection = sqlite3.connect(path)
        try:
            cursor = connection.execute(
                "SELECT typeof(reference), reference FROM test_reference"
            )
            return cursor.fetchall()

        finally:
            connection.close()

    def _RegisterSQLiteAttributeSerializer(self):
        """Registers the SQLite attribute serializer of the test data type."""
        schema.SchemaHelper.DeregisterDataType("test.Reference")
        schema.SchemaHelper.RegisterDataType(
            "test.Reference",
            {
                "json": _TestJSONReferenceAttributeSerializer(),
                "sqlite": _TestSQLiteReferenceAttributeSerializer(),
            },
        )

    def testWriteWithJSONAttributeSerializer(self):
        """Tests writing with a JSON attribute serializer."""
        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                test_store.AddAttributeContainer(self._CreateTestContainer(1))

            finally:
                test_store.Close()

            column_values = self._GetColumnValues(test_path)
            self.assertEqual(column_values, [("text", '"test_container.1"')])

    def testWriteWithSQLiteAttributeSerializer(self):
        """Tests writing with a SQLite attribute serializer."""
        self._RegisterSQLiteAttributeSerializer()

        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                test_store.AddAttributeContainer(self._CreateTestContainer(1))
                test_store.AddAttributeContainer(self._CreateTestContainer(2))

            finally:
                test_store.Close()

            column_values = self._GetColumnValues(test_path)
            self.assertEqual(column_values, [("integer", 1), ("integer", 2)])

            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path)

            try:
                containers = list(test_store.GetAttributeContainers("test_reference"))
                self.assertEqual(
                    [container.reference.CopyToString() for container in containers],
                    ["test_container.1", "test_container.2"],
                )

                containers = list(
                    test_store.GetAttributeContainers(
                        "test_reference", filter_expression="reference == 2"
                    )
                )
                self.assertEqual(len(containers), 1)
                self.assertEqual(containers[0].GetIdentifier().sequence_number, 2)

                container = test_store.GetAttributeContainerByIndex("test_reference", 1)
                self.assertEqual(container.reference.sequence_number, 2)

            finally:
                test_store.Close()

    def testAppendWithSQLiteAttributeSerializer(self):
        """Tests appending with a SQLite attribute serializer to JSON values."""
        with test_lib.TempDirectory() as temp_directory:
            test_path = os.path.join(temp_directory, "acstore.sqlite")
            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                test_store.AddAttributeContainer(self._CreateTestContainer(1))
                test_store.AddAttributeContainer(self._CreateTestContainer(2))

            finally:
                test_store.Close()

            self._RegisterSQLiteAttributeSerializer()

            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path, read_only=False)

            try:
                test_store.AddAttributeContainer(self._CreateTestContainer(3))

                container = test_store.GetAttributeContainerByIndex("test_reference", 1)
                self.assertEqual(container.reference.CopyToString(), "test_container.2")

                container.reference.sequence_number = 4
                test_store.UpdateAttributeContainer(container)

            finally:
                test_store.Close()

            # The column of the existing table has TEXT affinity.
            column_values = self._GetColumnValues(test_path)
            self.assertEqual(
                column_values,
                [("text", '"test_container.1"'), ("text", "4"), ("text", "3")],
            )

            test_store = sqlite_store.SQLiteAttributeContainerStore()
            test_store.Open(path=test_path)

            try:
                containers = list(test_store.GetAttributeContainers("test_reference"))
                self.assertEqual(
                    [container.reference.CopyToString() for container in containers],
                    ["test_container.1", "test_container.4", "test_container.3"],
                )

                containers = list(
                    test_store.GetAttributeContainers(
                        "test_reference", filter_expression="reference == 3"
                    )
                )
                self.assertEqual(len(containers), 1)
                self.assertEqual(containers[0].GetIdentifier().sequence_number, 3)

            finally:
                test_store.Close()


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the fake (in-memory only) store."""

import unittest

from acstore import fake_store

from tests import test_lib


class FakeAttributeContainerStoreTest(test_lib.BaseTestCase):
  """Tests for the fake (in-memory only) store."""

  # pylint: disable=protected-access

  def testRaiseIfNotReadable(self):
    """Tests the _RaiseIfNotReadable function."""
    test_store = fake_store.FakeAttributeContainerStore()

    with self.assertRaises(IOError):
      test_store._RaiseIfNotReadable()

  def testRaiseIfNotWritable(self):
    """Tests the _RaiseIfNotWritable function."""
    test_store = fake_store.FakeAttributeContainerStore()

    with self.assertRaises(IOError):
      test_store._RaiseIfNotWritable()

  def testWriteExistingAttributeContainer(self):
    """Tests the _WriteExistingAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    test_store = fake_store.FakeAttributeContainerStore()
    test_store.Open()

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 0)

    with self.assertRaises(IOError):
      test_store._WriteExistingAttributeContainer(attribute_container)

    test_store._WriteNewAttributeContainer(attribute_container)

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 1)

    test_store._WriteExistingAttributeContainer(attribute_container)

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 1)

    test_store.Close()

  def testWriteNewAttributeContainer(self):
    """Tests the _WriteNewAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    test_store = fake_store.FakeAttributeContainerStore()
    test_store.Open()

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 0)

    test_store._WriteNewAttributeContainer(attribute_container)

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 1)

    test_store.Close()

  def testAddAttributeContainer(self):
    """Tests the AddAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    test_store = fake_store.FakeAttributeContainerStore()
    test_store.Open()

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 0)

    test_store.AddAttributeContainer(attribute_container)

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 1)

    test_store.Close()

    with self.assertRaises(IOError):
      test_store.AddAttributeContainer(attribute_container)

  def testGetAttributeContainerByIdentifier(self):
    """Tests the GetAttributeContainerByIdentifier function."""
    attribute_container = test_lib.TestAttributeContainer()

    test_store = fake_store.FakeAttributeContainerStore()
    test_store.Open()

    test_store.AddAttributeContainer(attribute_container)
    identifier = attribute_container.GetIdentifier()

    container = test_store.GetAttributeContainerByIdentifier(
        attribute_container.CONTAINER_TYPE, identifier)
    self.assertIsNotNone(container)

    identifier.sequence_number = 99

    container = test_store.GetAttributeContainerByIdentifier(
        attribute_container.CONTAINER_TYPE, identifier)
    self.assertIsNone(container)

    test_store.Close()

  def testGetAttributeContainerByIndex(self):
    """Tests the GetAttributeContainerByIndex function."""
    attribute_container = test_lib.TestAttributeContainer()

    test_store = fake_store.FakeAttributeContainerStore()
    test_store.Open()

    container = test_store.GetAttributeContainerByIndex(
        attribute_container.CONTAINER_TYPE, 0)
    self.assertIsNone(container)

    test_store.AddAttributeContainer(attribute_container)

    container = test_store.GetAttributeContainerByIndex(
        attribute_container.CONTAINER_TYPE, 0)
    self.assertIsNotNone(container)

    test_store.Close()

  def testGetAttributeContainers(self):
    """Tests the GetAttributeContainers function."""
    attribute_container = test_lib.TestAttributeContainer()
    attribute_container.attribute = '8f0bf95a7959baad9666b21a7feed79d'

    test_store = fake_store.FakeAttributeContainerStore()
    test_store.Open()

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

    test_store.Close()

  def testGetNumberOfAttributeContainers(self):
    """Tests the GetNumberOfAttributeContainers function."""
    attribute_container = test_lib.TestAttributeContainer()

    test_store = fake_store.FakeAttributeContainerStore()
    test_store.Open()

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 0)

    test_store.AddAttributeContainer(attribute_container)

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 1)

    test_store.Close()

  def testHasAttributeContainers(self):
    """Tests the HasAttributeContainers function."""
    attribute_container = test_lib.TestAttributeContainer()

    test_store = fake_store.FakeAttributeContainerStore()
    test_store.Open()

    result = test_store.HasAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertFalse(result)

    test_store.AddAttributeContainer(attribute_container)

    result = test_store.HasAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertTrue(result)

    test_store.Close()

  def testOpenClose(self):
    """Tests the Open and Close functions."""
    test_store = fake_store.FakeAttributeContainerStore()
    test_store.Open()
    test_store.Close()

    test_store.Open()
    test_store.Close()

    test_store = fake_store.FakeAttributeContainerStore()
    test_store.Open()
    test_store.Close()

    test_store.Open()

    with self.assertRaises(IOError):
      test_store.Open()

    test_store.Close()

    with self.assertRaises(IOError):
      test_store.Close()

  def testUpdateAttributeContainer(self):
    """Tests the UpdateAttributeContainer function."""
    attribute_container = test_lib.TestAttributeContainer()

    test_store = fake_store.FakeAttributeContainerStore()
    test_store.Open()

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 0)

    with self.assertRaises(IOError):
      test_store.UpdateAttributeContainer(attribute_container)

    test_store.AddAttributeContainer(attribute_container)

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 1)

    test_store.UpdateAttributeContainer(attribute_container)

    number_of_containers = test_store.GetNumberOfAttributeContainers(
        attribute_container.CONTAINER_TYPE)
    self.assertEqual(number_of_containers, 1)

    test_store.Close()


if __name__ == '__main__':
  unittest.main()

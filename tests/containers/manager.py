#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the attribute container manager."""

import unittest

from acstore.containers import manager

from tests import test_lib as shared_test_lib


class AttributeContainersManagerTest(shared_test_lib.BaseTestCase):
  """Tests for the attribute container manager."""

  # pylint: disable=protected-access

  _TEST_MANAGER = manager.AttributeContainersManager

  def testCreateAttributeContainer(self):
    """Tests the CreateAttributeContainer function."""
    self._TEST_MANAGER.RegisterAttributeContainer(
        shared_test_lib.TestAttributeContainer)

    try:
      attribute_container = self._TEST_MANAGER.CreateAttributeContainer(
          'test_container')
      self.assertIsNotNone(attribute_container)

      with self.assertRaises(ValueError):
        self._TEST_MANAGER.CreateAttributeContainer('bogus')

    finally:
      self._TEST_MANAGER.DeregisterAttributeContainer(
          shared_test_lib.TestAttributeContainer)

  def testGetContainerTypes(self):
    """Tests the GetContainerTypes function."""
    self._TEST_MANAGER.RegisterAttributeContainer(
        shared_test_lib.TestAttributeContainer)

    try:
      container_types = self._TEST_MANAGER.GetContainerTypes()
      self.assertIn('test_container', container_types)

    finally:
      self._TEST_MANAGER.DeregisterAttributeContainer(
          shared_test_lib.TestAttributeContainer)

  def testGetSchema(self):
    """Tests the GetSchema function."""
    self._TEST_MANAGER.RegisterAttributeContainer(
        shared_test_lib.TestAttributeContainer)

    try:
      schema = self._TEST_MANAGER.GetSchema('test_container')
      self.assertIsNotNone(schema)
      self.assertEqual(schema, shared_test_lib.TestAttributeContainer.SCHEMA)

      with self.assertRaises(ValueError):
        self._TEST_MANAGER.GetSchema('bogus')

    finally:
      self._TEST_MANAGER.DeregisterAttributeContainer(
          shared_test_lib.TestAttributeContainer)

  def testAttributeContainerRegistration(self):
    """Tests the Register and DeregisterAttributeContainer functions."""
    number_of_classes = len(self._TEST_MANAGER._attribute_container_classes)

    self._TEST_MANAGER.RegisterAttributeContainer(
        shared_test_lib.TestAttributeContainer)

    try:
      self.assertEqual(
          len(self._TEST_MANAGER._attribute_container_classes),
          number_of_classes + 1)

      with self.assertRaises(KeyError):
        self._TEST_MANAGER.RegisterAttributeContainer(
            shared_test_lib.TestAttributeContainer)

    finally:
      self._TEST_MANAGER.DeregisterAttributeContainer(
          shared_test_lib.TestAttributeContainer)

    self.assertEqual(
        len(self._TEST_MANAGER._attribute_container_classes),
        number_of_classes)


if __name__ == '__main__':
  unittest.main()

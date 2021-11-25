#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the attribute container manager."""

import unittest

from acstore.containers import manager

from tests import test_lib as shared_test_lib


class AttributeContainersManagerTest(shared_test_lib.BaseTestCase):
  """Tests for the attribute container manager."""

  # pylint: disable=protected-access

  def testCreateAttributeContainer(self):
    """Tests the CreateAttributeContainer function."""
    manager.AttributeContainersManager.RegisterAttributeContainer(
        shared_test_lib.TestAttributeContainer)

    try:
      attribute_container = (
          manager.AttributeContainersManager.CreateAttributeContainer(
              'test_container'))
      self.assertIsNotNone(attribute_container)

      with self.assertRaises(ValueError):
        manager.AttributeContainersManager.CreateAttributeContainer('bogus')

    finally:
      manager.AttributeContainersManager.DeregisterAttributeContainer(
          shared_test_lib.TestAttributeContainer)

  def testGetContainerTypes(self):
    """Tests the GetContainerTypes function."""
    manager.AttributeContainersManager.RegisterAttributeContainer(
        shared_test_lib.TestAttributeContainer)

    try:
      container_types = manager.AttributeContainersManager.GetContainerTypes()
      self.assertIn('test_container', container_types)

    finally:
      manager.AttributeContainersManager.DeregisterAttributeContainer(
          shared_test_lib.TestAttributeContainer)

  def testGetSchema(self):
    """Tests the GetSchema function."""
    manager.AttributeContainersManager.RegisterAttributeContainer(
        shared_test_lib.TestAttributeContainer)

    try:
      schema = manager.AttributeContainersManager.GetSchema('test_container')
      self.assertIsNotNone(schema)
      self.assertEqual(schema, shared_test_lib.TestAttributeContainer.SCHEMA)

      with self.assertRaises(ValueError):
        manager.AttributeContainersManager.GetSchema('bogus')

    finally:
      manager.AttributeContainersManager.DeregisterAttributeContainer(
          shared_test_lib.TestAttributeContainer)

  def testAttributeContainerRegistration(self):
    """Tests the Register and DeregisterAttributeContainer functions."""
    number_of_classes = len(
        manager.AttributeContainersManager._attribute_container_classes)

    manager.AttributeContainersManager.RegisterAttributeContainer(
        shared_test_lib.TestAttributeContainer)

    try:
      self.assertEqual(
          len(manager.AttributeContainersManager._attribute_container_classes),
          number_of_classes + 1)

      with self.assertRaises(KeyError):
        manager.AttributeContainersManager.RegisterAttributeContainer(
            shared_test_lib.TestAttributeContainer)

    finally:
      manager.AttributeContainersManager.DeregisterAttributeContainer(
          shared_test_lib.TestAttributeContainer)

    self.assertEqual(
        len(manager.AttributeContainersManager._attribute_container_classes),
        number_of_classes)


if __name__ == '__main__':
  unittest.main()

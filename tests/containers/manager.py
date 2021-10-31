#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the attribute container manager."""

import unittest

from acstore.containers import interface
from acstore.containers import manager

from tests import test_lib as shared_test_lib


class TestAttributeContainer(interface.AttributeContainer):
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


class AttributeContainersManagerTest(shared_test_lib.BaseTestCase):
  """Tests for the attribute container manager."""

  # pylint: disable=protected-access

  def testCreateAttributeContainer(self):
    """Tests the CreateAttributeContainer function."""
    manager.AttributeContainersManager.RegisterAttributeContainer(
        TestAttributeContainer)

    try:
      attribute_container = (
          manager.AttributeContainersManager.CreateAttributeContainer(
              'test_container'))
      self.assertIsNotNone(attribute_container)

      with self.assertRaises(ValueError):
        manager.AttributeContainersManager.CreateAttributeContainer('bogus')

    finally:
      manager.AttributeContainersManager.DeregisterAttributeContainer(
          TestAttributeContainer)

  def testGetContainerTypes(self):
    """Tests the GetContainerTypes function."""
    manager.AttributeContainersManager.RegisterAttributeContainer(
        TestAttributeContainer)

    try:
      container_types = manager.AttributeContainersManager.GetContainerTypes()
      self.assertIn('test_container', container_types)

    finally:
      manager.AttributeContainersManager.DeregisterAttributeContainer(
          TestAttributeContainer)

  def testGetSchema(self):
    """Tests the GetSchema function."""
    manager.AttributeContainersManager.RegisterAttributeContainer(
        TestAttributeContainer)

    try:
      schema = manager.AttributeContainersManager.GetSchema('test_container')
      self.assertIsNotNone(schema)
      self.assertEqual(schema, TestAttributeContainer.SCHEMA)

      with self.assertRaises(ValueError):
        manager.AttributeContainersManager.GetSchema('bogus')

    finally:
      manager.AttributeContainersManager.DeregisterAttributeContainer(
          TestAttributeContainer)

  def testAttributeContainerRegistration(self):
    """Tests the Register and DeregisterAttributeContainer functions."""
    number_of_classes = len(
        manager.AttributeContainersManager._attribute_container_classes)

    manager.AttributeContainersManager.RegisterAttributeContainer(
        TestAttributeContainer)

    try:
      self.assertEqual(
          len(manager.AttributeContainersManager._attribute_container_classes),
          number_of_classes + 1)

      with self.assertRaises(KeyError):
        manager.AttributeContainersManager.RegisterAttributeContainer(
            TestAttributeContainer)

    finally:
      manager.AttributeContainersManager.DeregisterAttributeContainer(
          TestAttributeContainer)

    self.assertEqual(
        len(manager.AttributeContainersManager._attribute_container_classes),
        number_of_classes)


if __name__ == '__main__':
  unittest.main()

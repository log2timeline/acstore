#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the attribute container JSON serializer."""

import unittest

from acstore.containers import manager
from acstore.helpers import json_serializer

from tests import test_lib as shared_test_lib


class AttributeContainerJSONSerializerTest(shared_test_lib.BaseTestCase):
  """Tests for the attribute container JSON serializer."""

  _TEST_MANAGER = manager.AttributeContainersManager
  _TEST_SERIALIZER = json_serializer.AttributeContainerJSONSerializer

  def testConvertAttributeContainerToJSON(self):
    """Tests the ConvertAttributeContainerToJSON function."""
    attribute_container = shared_test_lib.TestAttributeContainer()
    attribute_container.attribute = 'MyAttribute'

    expected_json_dict = {
        '__container_type__': 'test_container',
        '__type__': 'AttributeContainer',
        'attribute': 'MyAttribute'}

    json_dict = self._TEST_SERIALIZER.ConvertAttributeContainerToJSON(
        attribute_container)
    self.assertEqual(json_dict, expected_json_dict)

  def testConvertJSONToAttributeContainer(self):
    """Tests the ConvertJSONToAttributeContainer function."""
    json_dict = {
        '__container_type__': 'test_container',
        '__type__': 'AttributeContainer',
        'attribute': 'MyAttribute'}

    self._TEST_MANAGER.RegisterAttributeContainer(
        shared_test_lib.TestAttributeContainer)

    try:
      attribute_container = (
          self._TEST_SERIALIZER.ConvertJSONToAttributeContainer(json_dict))

    finally:
      self._TEST_MANAGER.DeregisterAttributeContainer(
          shared_test_lib.TestAttributeContainer)

    self.assertIsNotNone(attribute_container)
    self.assertEqual(attribute_container.CONTAINER_TYPE, 'test_container')
    self.assertEqual(attribute_container.attribute, 'MyAttribute')


if __name__ == '__main__':
  unittest.main()

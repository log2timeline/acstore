#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the schema helper."""

import unittest

from acstore.helpers import schema

from tests import test_lib as shared_test_lib


class SchemaHelperTest(shared_test_lib.BaseTestCase):
  """Tests for the schema helper."""

  # pylint: disable=protected-access

  def testHasDataType(self):
    """Tests the HasDataType function."""
    result = schema.SchemaHelper.HasDataType('str')
    self.assertTrue(result)

    result = schema.SchemaHelper.HasDataType('test')
    self.assertFalse(result)

  def testRegisterDataType(self):
    """Tests the RegisterDataType function."""
    number_of_data_types = len(schema.SchemaHelper._data_types)

    schema.SchemaHelper.RegisterDataType('test', {'json': None})

    try:
      self.assertEqual(
          len(schema.SchemaHelper._data_types), number_of_data_types + 1)

      with self.assertRaises(KeyError):
        schema.SchemaHelper.RegisterDataType('test', {'json': None})

    finally:
      schema.SchemaHelper.DeregisterDataType('test')

    self.assertEqual(
        len(schema.SchemaHelper._data_types), number_of_data_types)

  def testRegisterDataTypes(self):
    """Tests the RegisterDataTypes function."""
    number_of_data_types = len(schema.SchemaHelper._data_types)

    schema.SchemaHelper.RegisterDataTypes({'test': {'json': None}})

    try:
      self.assertEqual(
          len(schema.SchemaHelper._data_types), number_of_data_types + 1)

      with self.assertRaises(KeyError):
        schema.SchemaHelper.RegisterDataTypes({'test': {'json': None}})

    finally:
      schema.SchemaHelper.DeregisterDataType('test')

    self.assertEqual(
        len(schema.SchemaHelper._data_types), number_of_data_types)


if __name__ == '__main__':
  unittest.main()

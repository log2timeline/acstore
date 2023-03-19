#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the YAML-based attribute container definitions file."""

import io
import unittest

from acstore import errors
from acstore.helpers import yaml_definitions_file

from tests import test_lib as shared_test_lib


class YAMLAttributeContainerDefinitionsFileTest(shared_test_lib.BaseTestCase):
  """Tests for the YAML-based attribute container definitions file."""

  # pylint: disable=protected-access

  _FORMATTERS_YAML = {
      'name': 'windows_eventlog_message_file',
      'attributes': [
          {'name': 'path', 'type': 'str'},
          {'name': 'windows_path', 'type': 'str'}]}

  def testReadDefinition(self):
    """Tests the _ReadDefinition function."""
    test_definitions_file = (
        yaml_definitions_file.YAMLAttributeContainerDefinitionsFile())

    container_class = test_definitions_file._ReadDefinition(
        self._FORMATTERS_YAML)

    self.assertIsNotNone(container_class)
    self.assertEqual(
        container_class.CONTAINER_TYPE, 'windows_eventlog_message_file')
    self.assertEqual(
        container_class.SCHEMA, {'path': 'str', 'windows_path': 'str'})

    with self.assertRaises(errors.ParseError):
      test_definitions_file._ReadDefinition({})

    with self.assertRaises(errors.ParseError):
      test_definitions_file._ReadDefinition({
          'name': 'windows_eventlog_message_file',
          'attributes': []})

    with self.assertRaises(errors.ParseError):
      test_definitions_file._ReadDefinition({
          'name': 'windows_eventlog_message_file',
          'attributes': [{'type': 'str'}]})

    with self.assertRaises(errors.ParseError):
      test_definitions_file._ReadDefinition({
          'name': 'windows_eventlog_message_file',
          'attributes': [{'name': 'path'}]})

    with self.assertRaises(errors.ParseError):
      test_definitions_file._ReadDefinition({
          'name': 'windows_eventlog_message_file',
          'attributes': [{'name': 'path', 'type': 'bogus'}]})

  def testReadFromFileObject(self):
    """Tests the _ReadFromFileObject function."""
    test_file_path = self._GetTestFilePath(['definitions.yaml'])
    self._SkipIfPathNotExists(test_file_path)

    test_definitions_file = (
        yaml_definitions_file.YAMLAttributeContainerDefinitionsFile())

    with io.open(test_file_path, 'r', encoding='utf-8') as file_object:
      definitions = list(test_definitions_file._ReadFromFileObject(file_object))

    self.assertEqual(len(definitions), 1)

  def testReadFromFile(self):
    """Tests the ReadFromFile function."""
    test_file_path = self._GetTestFilePath(['definitions.yaml'])
    self._SkipIfPathNotExists(test_file_path)

    test_definitions_file = (
        yaml_definitions_file.YAMLAttributeContainerDefinitionsFile())

    definitions = list(test_definitions_file.ReadFromFile(test_file_path))

    self.assertEqual(len(definitions), 1)

    self.assertEqual(
        definitions[0].CONTAINER_TYPE, 'windows_eventlog_message_file')


if __name__ == '__main__':
  unittest.main()

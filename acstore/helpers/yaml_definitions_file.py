# -*- coding: utf-8 -*-
"""YAML-based attribute container definitions file."""

import yaml

from acstore import errors
from acstore.containers import interface
from acstore.helpers import schema


# TODO: merge this into interface.AttributeContainer once Plaso has been
# changed to no longer support attributes containers without a schema.
class AttributeContainerWithSchema(interface.AttributeContainer):
  """Attribute container with schema."""

  SCHEMA = {}


class YAMLAttributeContainerDefinitionsFile(object):
  """YAML-based attribute container definitions file.

  A YAML-based attribute container definitions file contains one or more
  attribute container definitions. An attribute container definition consists
  of:

  name: 'windows_eventlog_message_file'
  attributes:
  - name: path
    type: str
  - name: windows_path
    type: str

  Where:
  * name, unique identifier of the attribute container;
  * attributes, defines the attributes of the container.
  """

  _SUPPORTED_DATA_TYPES = frozenset([
      'AttributeContainerIdentifier',
      'bool',
      'int',
      'str',
      'timestamp'])

  _SUPPORTED_KEYS = frozenset([
      'name',
      'attributes'])

  def _ReadDefinition(self, definition_values):
    """Reads a definition from a dictionary.

    Args:
      definition_values (dict[str, object]): attribute container definition
          values.

    Returns:
      AttributeContainer: an attribute container.

    Raises:
      ParseError: if the definition is not set or incorrect.
    """
    if not definition_values:
      raise errors.ParseError('Missing attribute container definition values.')

    different_keys = set(definition_values) - self._SUPPORTED_KEYS
    if different_keys:
      different_keys = ', '.join(different_keys)
      raise errors.ParseError(f'Undefined keys: {different_keys:s}')

    container_name = definition_values.get('name', None)
    if not container_name:
      raise errors.ParseError(
          'Invalid attribute container definition missing name.')

    attributes = definition_values.get('attributes', None)
    if not attributes:
      raise errors.ParseError((
          f'Invalid attribute container definition: {container_name:s} '
          f'missing attributes.'))

    class_name = ''.join([
        element.title() for element in container_name.split('_')])

    class_attributes = {'CONTAINER_TYPE': container_name}
    container_schema = {}

    for attribute_index, attribute_values in enumerate(attributes):
      attribute_name = attribute_values.get('name', None)
      if not attribute_name:
        raise errors.ParseError((
            f'Invalid attribute container definition: {container_name:s} name '
            f'missing of attribute: {attribute_index:d}.'))

      if attribute_name in class_attributes:
        raise errors.ParseError((
            f'Invalid attribute container definition: {container_name:s} '
            f'attribute: {attribute_name:s} already set.'))

      attribute_data_type = attribute_values.get('type', None)
      if not attribute_data_type:
        raise errors.ParseError((
            f'Invalid attribute container definition: {container_name:s} type '
            f'missing of attribute: {attribute_name:s}.'))

      if not schema.SchemaHelper.HasDataType(attribute_data_type):
        raise errors.ParseError((
            f'Invalid attribute container definition: {container_name:s} type '
            f'attribute: {attribute_name:s} unsupported data type: '
            f'{attribute_data_type:s}.'))

      class_attributes[attribute_name] = None
      container_schema[attribute_name] = attribute_data_type

    class_attributes['SCHEMA'] = container_schema

    # TODO: add support for _SERIALIZABLE_PROTECTED_ATTRIBUTES.

    return type(class_name, (AttributeContainerWithSchema, ), class_attributes)

  def _ReadFromFileObject(self, file_object):
    """Reads the definitions from a file-like object.

    Args:
      file_object (file): definitions file-like object.

    Yields:
      AttributeContainer: an attribute container.
    """
    yaml_generator = yaml.safe_load_all(file_object)

    for yaml_definition in yaml_generator:
      yield self._ReadDefinition(yaml_definition)

  def ReadFromFile(self, path):
    """Reads the definitions from a YAML file.

    Args:
      path (str): path to a definitions file.

    Yields:
      AttributeContainer: an attribute container.
    """
    with open(path, 'r', encoding='utf-8') as file_object:
      for yaml_definition in self._ReadFromFileObject(file_object):
        yield yaml_definition

# -*- coding: utf-8 -*-
"""LevelDB-based attribute container store."""

import ast
import json
import os

import leveldb  # pylint: disable=import-error

from acstore import interface
from acstore.containers import interface as containers_interface
from acstore.helpers import json_serializer


class LevelDBAttributeContainerStore(
    interface.AttributeContainerStoreWithReadCache):
  """LevelDB-based attribute container store.

  Attributes:
    format_version (int): storage format version.
    serialization_format (str): serialization format.
  """

  _FORMAT_VERSION = 20230312

  def __init__(self):
    """Initializes a LevelDB attribute container store."""
    super(LevelDBAttributeContainerStore, self).__init__()
    self._is_open = False
    self._json_serializer = json_serializer.AttributeContainerJSONSerializer
    self._leveldb_database = None

    self.format_version = self._FORMAT_VERSION
    self.serialization_format = 'json'

  def _GetNumberOfAttributeContainerKeys(self, container_type):
    """Retrieves the number of attribute container keys.

    Args:
      container_type (str): attribute container type.

    Returns:
      int: the number of keys of a specified attribute container type.
    """
    first_key = f'{container_type:s}.1'.encode('utf8')

    try:
      # Check if the first key exists otherwise RangeIter will return all keys.
      self._leveldb_database.Get(first_key)
    except KeyError:
      return 0

    return sum(1 for _ in self._leveldb_database.RangeIter(key_from=first_key))

  def _RaiseIfNotReadable(self):
    """Raises if the attribute container store is not readable.

    Raises:
      IOError: when the attribute container store is closed.
      OSError: when the attribute container store is closed.
    """
    if not self._is_open:
      raise IOError('Unable to read from closed attribute container store.')

  def _RaiseIfNotWritable(self):
    """Raises if the attribute container store is not writable.

    Raises:
      IOError: when the attribute container store is closed or read-only.
      OSError: when the attribute container store is closed or read-only.
    """
    if not self._is_open:
      raise IOError('Unable to write to closed attribute container store.')

  def _WriteExistingAttributeContainer(self, container):
    """Writes an existing attribute container to the store.

    Args:
      container (AttributeContainer): attribute container.
    """
    identifier = container.GetIdentifier()

    key = identifier.CopyToString().encode('utf8')

    self._leveldb_database.Delete(key)

    json_dict = self._json_serializer.ConvertAttributeContainerToJSON(container)
    json_string = json.dumps(json_dict)
    value = json_string.encode('utf8')

    self._leveldb_database.Put(key=key, value=value)

  def _WriteNewAttributeContainer(self, container):
    """Writes a new attribute container to the store.

    Args:
      container (AttributeContainer): attribute container.
    """
    next_sequence_number = self._GetAttributeContainerNextSequenceNumber(
        container.CONTAINER_TYPE)

    identifier = containers_interface.AttributeContainerIdentifier(
        name=container.CONTAINER_TYPE, sequence_number=next_sequence_number)
    container.SetIdentifier(identifier)

    key = identifier.CopyToString().encode('utf8')

    json_dict = self._json_serializer.ConvertAttributeContainerToJSON(container)
    json_string = json.dumps(json_dict)
    value = json_string.encode('utf8')

    self._leveldb_database.Put(key=key, value=value)

    self._CacheAttributeContainerByIndex(container, next_sequence_number - 1)

  def Close(self):
    """Closes the file.

    Raises:
      IOError: if the attribute container store is already closed.
      OSError: if the attribute container store is already closed.
    """
    if not self._is_open:
      raise IOError('Attribute container store already closed.')

    self._leveldb_database = None

    self._is_open = False

  def GetAttributeContainerByIdentifier(self, container_type, identifier):
    """Retrieves a specific type of container with a specific identifier.

    Args:
      container_type (str): container type.
      identifier (AttributeContainerIdentifier): attribute container identifier.

    Returns:
      AttributeContainer: attribute container or None if not available.
    """
    key = identifier.CopyToString().encode('utf8')

    try:
      value = self._leveldb_database.Get(key)
    except KeyError:
      return None

    json_string = value.decode('utf8')
    json_dict = json.loads(json_string)

    container = self._json_serializer.ConvertJSONToAttributeContainer(json_dict)
    container.SetIdentifier(identifier)
    return container

  def GetAttributeContainerByIndex(self, container_type, index):
    """Retrieves a specific attribute container.

    Args:
      container_type (str): attribute container type.
      index (int): attribute container index.

    Returns:
      AttributeContainer: attribute container or None if not available.
    """
    identifier = containers_interface.AttributeContainerIdentifier(
        name=container_type, sequence_number=index + 1)

    key = identifier.CopyToString().encode('utf8')

    try:
      value = self._leveldb_database.Get(key)
    except KeyError:
      return None

    json_string = value.decode('utf8')
    json_dict = json.loads(json_string)

    container = self._json_serializer.ConvertJSONToAttributeContainer(json_dict)
    container.SetIdentifier(identifier)
    return container

  def GetAttributeContainers(self, container_type, filter_expression=None):
    """Retrieves a specific type of attribute containers.

    Args:
      container_type (str): attribute container type.
      filter_expression (Optional[str]): expression to filter the resulting
          attribute containers by.

    Yields:
      AttributeContainer: attribute container.
    """
    last_key_index = self._attribute_container_sequence_numbers[container_type]

    first_key = f'{container_type:s}.1'.encode('utf8')
    last_key = f'{container_type:s}.{last_key_index:d}'.encode('utf8')

    if filter_expression:
      expression_ast = ast.parse(filter_expression, mode='eval')
      filter_expression = compile(expression_ast, '<string>', mode='eval')

    for key, value in self._leveldb_database.RangeIter(
        key_from=first_key, key_to=last_key):
      json_string = value.decode('utf8')
      json_dict = json.loads(json_string)

      container = self._json_serializer.ConvertJSONToAttributeContainer(
          json_dict)
      if container.MatchesExpression(filter_expression):
        key = key.decode('utf8')
        identifier = containers_interface.AttributeContainerIdentifier()
        identifier.CopyFromString(key)

        container.SetIdentifier(identifier)
        yield container

  def GetNumberOfAttributeContainers(self, container_type):
    """Retrieves the number of a specific type of attribute containers.

    Args:
      container_type (str): attribute container type.

    Returns:
      int: the number of containers of a specified type.
    """
    return self._attribute_container_sequence_numbers[container_type]

  def HasAttributeContainers(self, container_type):
    """Determines if a store contains a specific type of attribute container.

    Args:
      container_type (str): attribute container type.

    Returns:
      bool: True if the store contains the specified type of attribute
          containers.
    """
    return self._attribute_container_sequence_numbers[container_type] > 0

  def Open(self, path=None, **unused_kwargs):  # pylint: disable=arguments-differ
    """Opens the store.

    Args:
      path (Optional[str]): path to the attribute container store.

    Raises:
      IOError: if the attribute container store is already opened or if
          the database cannot be connected.
      OSError: if the attribute container store is already opened or if
          the database cannot be connected.
      ValueError: if path is missing.
    """
    if self._is_open:
      raise IOError('Attribute container store already opened.')

    if not path:
      raise ValueError('Missing path.')

    path = os.path.abspath(path)

    self._leveldb_database = leveldb.LevelDB(path)

    self._is_open = True

    # TODO: read metadata.

    # Initialize next_sequence_number based on the file contents so that
    # AttributeContainerIdentifier points to the correct attribute container.
    for container_type in self._containers_manager.GetContainerTypes():
      next_sequence_number = self._GetNumberOfAttributeContainerKeys(
          container_type)
      self._SetAttributeContainerNextSequenceNumber(
          container_type, next_sequence_number)

# -*- coding: utf-8 -*-
"""The attribute container store interface."""

import abc
import collections

from acstore.containers import manager as containers_manager


class AttributeSerializer(object):
  """Attribute serializer."""

  @abc.abstractmethod
  def DeserializeValue(self, value):
    """Deserializes a value.

    Args:
      value (object): serialized value.

    Returns:
      object: runtime value.
    """

  @abc.abstractmethod
  def SerializeValue(self, value):
    """Serializes a value.

    Args:
      value (object): runtime value.

    Returns:
      object: serialized value.
    """


class AttributeContainerStore(object):
  """Interface of an attribute container store.

  Attributes:
    format_version (int): storage format version.
  """

  def __init__(self):
    """Initializes an attribute container store."""
    super(AttributeContainerStore, self).__init__()
    self._attribute_container_sequence_numbers = collections.Counter()
    self._containers_manager = containers_manager.AttributeContainersManager
    self._storage_profiler = None

    self.format_version = None

  def _GetAttributeContainerNextSequenceNumber(self, container_type):
    """Retrieves the next sequence number of an attribute container.

    Args:
      container_type (str): attribute container type.

    Returns:
      int: next sequence number.
    """
    self._attribute_container_sequence_numbers[container_type] += 1
    return self._attribute_container_sequence_numbers[container_type]

  def _GetAttributeContainerSchema(self, container_type):
    """Retrieves the schema of an attribute container.

    Args:
      container_type (str): attribute container type.

    Returns:
      dict[str, str]: attribute container schema or an empty dictionary if
          no schema available.
    """
    try:
      schema = self._containers_manager.GetSchema(container_type)
    except ValueError:
      schema = {}

    return schema

  @abc.abstractmethod
  def _RaiseIfNotReadable(self):
    """Raises if the store is not readable.

    Raises:
      OSError: if the store cannot be read from.
      IOError: if the store cannot be read from.
    """

  @abc.abstractmethod
  def _RaiseIfNotWritable(self):
    """Raises if the store is not writable.

    Raises:
      OSError: if the store cannot be written to.
      IOError: if the store cannot be written to.
    """

  def _SetAttributeContainerNextSequenceNumber(
      self, container_type, next_sequence_number):
    """Sets the next sequence number of an attribute container.

    Args:
      container_type (str): attribute container type.
      next_sequence_number (int): next sequence number.
    """
    self._attribute_container_sequence_numbers[
        container_type] = next_sequence_number

  @abc.abstractmethod
  def _WriteExistingAttributeContainer(self, container):
    """Writes an existing attribute container to the store.

    Args:
      container (AttributeContainer): attribute container.
    """

  @abc.abstractmethod
  def _WriteNewAttributeContainer(self, container):
    """Writes a new attribute container to the store.

    Args:
      container (AttributeContainer): attribute container.
    """

  def AddAttributeContainer(self, container):
    """Adds a new attribute container.

    Args:
      container (AttributeContainer): attribute container.

    Raises:
      OSError: if the store cannot be written to.
      IOError: if the store cannot be written to.
    """
    self._RaiseIfNotWritable()
    self._WriteNewAttributeContainer(container)

  @abc.abstractmethod
  def Close(self):
    """Closes the store."""

  @abc.abstractmethod
  def GetAttributeContainerByIdentifier(self, container_type, identifier):
    """Retrieves a specific type of container with a specific identifier.

    Args:
      container_type (str): container type.
      identifier (AttributeContainerIdentifier): attribute container identifier.

    Returns:
      AttributeContainer: attribute container or None if not available.
    """

  @abc.abstractmethod
  def GetAttributeContainerByIndex(self, container_type, index):
    """Retrieves a specific attribute container.

    Args:
      container_type (str): attribute container type.
      index (int): attribute container index.

    Returns:
      AttributeContainer: attribute container or None if not available.
    """

  @abc.abstractmethod
  def GetAttributeContainers(self, container_type, filter_expression=None):
    """Retrieves a specific type of attribute containers.

    Args:
      container_type (str): attribute container type.
      filter_expression (Optional[str]): expression to filter the resulting
          attribute containers by.

    Returns:
      generator(AttributeContainer): attribute container generator.
    """

  @abc.abstractmethod
  def GetNumberOfAttributeContainers(self, container_type):
    """Retrieves the number of a specific type of attribute containers.

    Args:
      container_type (str): attribute container type.

    Returns:
      int: the number of containers of a specified type.
    """

  @abc.abstractmethod
  def HasAttributeContainers(self, container_type):
    """Determines if a store contains a specific type of attribute container.

    Args:
      container_type (str): attribute container type.

    Returns:
      bool: True if the store contains the specified type of attribute
          containers.
    """

  @abc.abstractmethod
  def Open(self, **kwargs):
    """Opens the store."""

  def SetStorageProfiler(self, storage_profiler):
    """Sets the storage profiler.

    Args:
      storage_profiler (StorageProfiler): storage profiler.
    """
    self._storage_profiler = storage_profiler

  def UpdateAttributeContainer(self, container):
    """Updates an existing attribute container.

    Args:
      container (AttributeContainer): attribute container.

    Raises:
      OSError: if the store cannot be written to.
      IOError: if the store cannot be written to.
    """
    self._RaiseIfNotWritable()
    self._WriteExistingAttributeContainer(container)


class AttributeContainerStoreWithReadCache(AttributeContainerStore):
  """Interface of an attribute container store with read cache.

  Attributes:
    format_version (int): storage format version.
  """

  # pylint: disable=abstract-method

  # The maximum number of cached attribute containers
  _MAXIMUM_CACHED_CONTAINERS = 32 * 1024

  def __init__(self):
    """Initializes an attribute container store with read cache."""
    super(AttributeContainerStoreWithReadCache, self).__init__()
    self._attribute_container_cache = collections.OrderedDict()

  def _CacheAttributeContainerByIndex(self, attribute_container, index):
    """Caches a specific attribute container.

    Args:
      attribute_container (AttributeContainer): attribute container.
      index (int): attribute container index.
    """
    if len(self._attribute_container_cache) >= self._MAXIMUM_CACHED_CONTAINERS:
      self._attribute_container_cache.popitem(last=True)

    lookup_key = f'{attribute_container.CONTAINER_TYPE:s}.{index:d}'
    self._attribute_container_cache[lookup_key] = attribute_container
    self._attribute_container_cache.move_to_end(lookup_key, last=False)

  def _GetCachedAttributeContainer(self, container_type, index):
    """Retrieves a specific cached attribute container.

    Args:
      container_type (str): attribute container type.
      index (int): attribute container index.

    Returns:
      AttributeContainer: attribute container or None if not available.
    """
    lookup_key = f'{container_type:s}.{index:d}'
    attribute_container = self._attribute_container_cache.get(lookup_key, None)
    if attribute_container:
      self._attribute_container_cache.move_to_end(lookup_key, last=False)
    return attribute_container

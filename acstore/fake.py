# -*- coding: utf-8 -*-
"""Fake (in-memory only) attribute container store for testing."""

import ast
import collections
import copy
import itertools

from acstore import interface
from acstore.containers import interface as containers_interface


class FakeAttributeContainerIdentifier(
    containers_interface.AttributeContainerIdentifier):
  """Fake attribute container identifier.

  Attributes:
    sequence_number (int): sequence number of the attribute container.
  """

  def __init__(self, sequence_number):
    """Initializes a fake attribute container identifier.

    Args:
      sequence_number (int): sequence number of the attribute container.
    """
    super(FakeAttributeContainerIdentifier, self).__init__()
    self.sequence_number = sequence_number

  def CopyToString(self):
    """Copies the identifier to a string representation.

    Returns:
      str: unique identifier or None.
    """
    if self.sequence_number is None:
      return None

    return '{0:d}'.format(self.sequence_number)


class FakeAttributeContainerStore(interface.AttributeContainerStore):
  """Fake (in-memory only) attribute container store."""

  def __init__(self):
    """Initializes a fake (in-memory only) store."""
    super(FakeAttributeContainerStore, self).__init__()
    self._attribute_containers = {}
    self._is_open = False

  def _RaiseIfNotReadable(self):
    """Raises if the store is not readable.

     Raises:
       OSError: if the store cannot be read from.
       IOError: if the store cannot be read from.
    """
    if not self._is_open:
      raise IOError('Unable to read from closed storage writer.')

  def _RaiseIfNotWritable(self):
    """Raises if the storage file is not writable.

    Raises:
      IOError: when the storage writer is closed.
      OSError: when the storage writer is closed.
    """
    if not self._is_open:
      raise IOError('Unable to write to closed storage writer.')

  def _WriteExistingAttributeContainer(self, container):
    """Writes an existing attribute container to the store.

    Args:
      container (AttributeContainer): attribute container.

    Raises:
      IOError: if an unsupported identifier is provided or if the attribute
          container does not exist.
      OSError: if an unsupported identifier is provided or if the attribute
          container does not exist.
    """
    identifier = container.GetIdentifier()
    if not isinstance(identifier, FakeAttributeContainerIdentifier):
      raise IOError(
          'Unsupported attribute container identifier type: {0!s}'.format(
              type(identifier)))

    lookup_key = identifier.CopyToString()

    containers = self._attribute_containers.get(container.CONTAINER_TYPE, None)
    if containers is None or lookup_key not in containers:
      raise IOError(
          'Missing attribute container: {0:s} with identifier: {1:s}'.format(
              container.CONTAINER_TYPE, lookup_key))

    containers[lookup_key] = container

  def _WriteNewAttributeContainer(self, container):
    """Writes a new attribute container to the store.

    Args:
      container (AttributeContainer): attribute container.
    """
    containers = self._attribute_containers.get(container.CONTAINER_TYPE, None)
    if containers is None:
      containers = collections.OrderedDict()
      self._attribute_containers[container.CONTAINER_TYPE] = containers

    next_sequence_number = self._GetAttributeContainerNextSequenceNumber(
        container.CONTAINER_TYPE)

    identifier = FakeAttributeContainerIdentifier(next_sequence_number)
    container.SetIdentifier(identifier)

    lookup_key = identifier.CopyToString()

    # Make sure the fake storage preserves the state of the attribute container.
    container = copy.deepcopy(container)
    containers[lookup_key] = container

  def Close(self):
    """Closes the store.

    Raises:
      IOError: if the store is already closed.
      OSError: if the store is already closed.
    """
    if not self._is_open:
      raise IOError('Store already closed.')

    self._is_open = False

  def GetAttributeContainerByIdentifier(self, container_type, identifier):
    """Retrieves a specific type of container with a specific identifier.

    Args:
      container_type (str): container type.
      identifier (AttributeContainerIdentifier): attribute container identifier.

    Returns:
      AttributeContainer: attribute container or None if not available.
    """
    containers = self._attribute_containers.get(container_type, {})

    lookup_key = identifier.CopyToString()
    return containers.get(lookup_key, None)

  def GetAttributeContainerByIndex(self, container_type, index):
    """Retrieves a specific attribute container.

    Args:
      container_type (str): attribute container type.
      index (int): attribute container index.

    Returns:
      AttributeContainer: attribute container or None if not available.
    """
    containers = self._attribute_containers.get(container_type, {})
    number_of_containers = len(containers)
    if index < 0 or index >= number_of_containers:
      return None

    return next(itertools.islice(
        containers.values(), index, number_of_containers))

  def GetAttributeContainers(self, container_type, filter_expression=None):
    """Retrieves a specific type of attribute containers.

    Args:
      container_type (str): attribute container type.
      filter_expression (Optional[str]): expression to filter the resulting
          attribute containers by.

    Yield:
      AttributeContainer: attribute container.
    """
    if filter_expression:
      expression_ast = ast.parse(filter_expression, mode='eval')
      filter_expression = compile(expression_ast, '<string>', mode='eval')

    for attribute_container in self._attribute_containers.get(
        container_type, {}).values():
      if attribute_container.MatchesExpression(filter_expression):
        yield attribute_container

  def GetNumberOfAttributeContainers(self, container_type):
    """Retrieves the number of a specific type of attribute containers.

    Args:
      container_type (str): attribute container type.

    Returns:
      int: the number of containers of a specified type.
    """
    containers = self._attribute_containers.get(container_type, {})
    return len(containers)

  def HasAttributeContainers(self, container_type):
    """Determines if a store contains a specific type of attribute container.

    Args:
      container_type (str): attribute container type.

    Returns:
      bool: True if the store contains the specified type of attribute
          containers.
    """
    containers = self._attribute_containers.get(container_type, {})
    return bool(containers)

  def Open(self, **kwargs):
    """Opens the store.

    Raises:
      IOError: if the store is already opened.
      OSError: if the store is already opened.
    """
    if self._is_open:
      raise IOError('Store already opened.')

    self._is_open = True

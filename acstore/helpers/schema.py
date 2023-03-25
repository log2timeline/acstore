# -*- coding: utf-8 -*-
"""Schema helper."""


class SchemaHelper(object):
  """Schema helper."""

  # Data types and corresponding attribute serializers per method.
  _data_types = {
      'AttributeContainerIdentifier': None,
      'bool': None,
      'int': None,
      'str': None,
      'timestamp': None}

  @classmethod
  def DeregisterDataType(cls, data_type):
    """Deregisters an data type.

    Args:
      data_type (str): data type.

    Raises:
      KeyError: if the data type is not set.
    """
    if data_type not in cls._data_types:
      raise KeyError(f'Data type: {data_type:s} not set.')

    del cls._data_types[data_type]

  @classmethod
  def GetAttributeSerializer(cls, data_type, serialization_method):
    """Retrieves a specific attribute serializer.

    Args:
      data_type (str): data type.
      serialization_method (str): serialization method.

    Returns:
      AttributeSerializer: attribute serializer or None if not available.
    """
    serializers = cls._data_types.get(data_type, None) or {}
    return serializers.get(serialization_method, None)

  @classmethod
  def HasDataType(cls, data_type):
    """Determines is a specific data type is supported by the schema.

    Args:
      data_type (str): data type.

    Returns:
      bool: True if the data type is supported, or False otherwise.
    """
    return data_type in cls._data_types

  @classmethod
  def RegisterDataType(cls, data_type, serializers):
    """Registers a data type.

    Args:
      data_type (str): data type.
      serializers (dict[str, AttributeSerializer]): attribute serializers per
          method.

    Raises:
      KeyError: if the data type is already set.
    """
    if data_type in cls._data_types:
      raise KeyError(f'Data type: {data_type:s} already set.')

    cls._data_types[data_type] = serializers

  @classmethod
  def RegisterDataTypes(cls, data_types):
    """Registers data types.

    Args:
      data_types (dict[str: dict[str, AttributeSerializer]]): attribute
          serializers with method per data types.

    Raises:
      KeyError: if the data type is already set.
    """
    for data_type, serializers in data_types.items():
      cls.RegisterDataType(data_type, serializers)

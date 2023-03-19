# -*- coding: utf-8 -*-
"""Schema helper."""


class SchemaHelper(object):
  """Schema helper."""

  _data_types = {
      'AttributeContainerIdentifier': None,
      'List[str]': None,
      'bool': None,
      'int': None,
      'str': None,
      'timestamp': None}

  @classmethod
  def DeregisterDataType(cls, data_type):
    """Deregisters an data type.

    Args:
      data_type (type): data type.

    Raises:
      KeyError: if the data type is not set.
    """
    if data_type not in cls._data_types:
      raise KeyError(f'Data type: {data_type:s} not set.')

    del cls._data_types[data_type]

  @classmethod
  def HasDataType(cls, data_type):
    """Determines is a specific data type is supported by the schema.

    Args:
      data_type (type): data type.

    Returns:
      bool: True if the data type is supported, or False otherwise.
    """
    return data_type in cls._data_types

  @classmethod
  def RegisterDataType(cls, data_type):
    """Registers a data type.

    Args:
      data_type (type): data type.

    Raises:
      KeyError: if the data type is already set.
    """
    if data_type in cls._data_types:
      raise KeyError(f'Data type: {data_type:s} already set.')

    cls._data_types[data_type] = data_type

  @classmethod
  def RegisterDataTypes(cls, data_types):
    """Registers data types.

    Args:
      data_types (list[type]): data types.

    Raises:
      KeyError: if the data type is already set.
    """
    for data_type in data_types:
      cls.RegisterDataType(data_type)

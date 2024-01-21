# -*- coding: utf-8 -*-
"""Attribute container JSON serializer."""

from acstore.containers import manager as containers_manager
from acstore.helpers import schema as schema_helper


class AttributeContainerJSONSerializer(object):
  """Attribute container JSON serializer."""

  _CONTAINERS_MANAGER = containers_manager.AttributeContainersManager

  @classmethod
  def ConvertAttributeContainerToJSON(cls, attribute_container):
    """Converts an attribute container object into a JSON dictioary.

    The resulting dictionary of the JSON serialized objects consists of:
    {
        '__type__': 'AttributeContainer'
        '__container_type__': ...
        ...
    }

    Here '__type__' indicates the object base type. In this case
    'AttributeContainer'.

    '__container_type__' indicates the container type and rest of the elements
    of the dictionary that make up the attributes of the container.

    Args:
      attribute_container (AttributeContainer): attribute container.

    Returns:
      dict[str, object]: JSON serialized objects.
    """
    try:
      schema = cls._CONTAINERS_MANAGER.GetSchema(
          attribute_container.CONTAINER_TYPE)
    except ValueError:
      schema = {}

    json_dict = {
        '__type__': 'AttributeContainer',
        '__container_type__': attribute_container.CONTAINER_TYPE}

    for attribute_name, attribute_value in attribute_container.GetAttributes():
      data_type = schema.get(attribute_name, None)
      if data_type:
        serializer = schema_helper.SchemaHelper.GetAttributeSerializer(
            data_type, 'json')

        attribute_value = serializer.SerializeValue(attribute_value)

      # JSON will not serialize certain runtime types like set, therefore
      # these are cast to list first.
      if isinstance(attribute_value, set):
        attribute_value = list(attribute_value)

      json_dict[attribute_name] = attribute_value

    return json_dict

  @classmethod
  def ConvertJSONToAttributeContainer(cls, json_dict):
    """Converts a JSON dictionary into an attribute container object.

    The dictionary of the JSON serialized objects consists of:
    {
        '__type__': 'AttributeContainer'
        '__container_type__': ...
        ...
    }

    Here '__type__' indicates the object base type. In this case
    'AttributeContainer'.

    '__container_type__' indicates the container type and rest of the elements
    of the dictionary that make up the attributes of the container.

    Args:
      json_dict (dict[str, object]): JSON serialized objects.

    Returns:
      AttributeContainer: attribute container.
    """
    # Use __container_type__ to indicate the attribute container type.
    container_type = json_dict.get('__container_type__', None)

    attribute_container = cls._CONTAINERS_MANAGER.CreateAttributeContainer(
        container_type)

    supported_attribute_names = attribute_container.GetAttributeNames()
    for attribute_name, attribute_value in json_dict.items():
      if attribute_name in ('__container_type__', '__type__'):
        continue

      # Be strict about which attributes to set.
      if attribute_name not in supported_attribute_names:
        continue

      setattr(attribute_container, attribute_name, attribute_value)

    return attribute_container

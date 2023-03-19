# -*- coding: utf-8 -*-
"""The error objects."""


class Error(Exception):
  """The error interface."""


class ParseError(Error):
  """Raised when a parse error occurred."""

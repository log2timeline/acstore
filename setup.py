#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Installation and deployment script."""

from __future__ import print_function
import sys

try:
  from setuptools import find_packages, setup
except ImportError:
  from distutils.core import find_packages, setup

try:
  from distutils.command.bdist_msi import bdist_msi
except ImportError:
  bdist_msi = None

try:
  from distutils.command.bdist_rpm import bdist_rpm
except ImportError:
  bdist_rpm = None

if sys.version < '2.7':
  print('Unsupported Python version: {0:s}.'.format(sys.version))
  print('Supported Python versions are 2.7 or a later 2.x version.')
  sys.exit(1)

# Change PYTHONPATH to include acstore so that we can get the version.
sys.path.insert(0, '.')

import acstore  # pylint: disable=wrong-import-position


if not bdist_msi:
  BdistMSICommand = None
else:
  class BdistMSICommand(bdist_msi):
    """Custom handler for the bdist_msi command."""

    def run(self):
      """Builds an MSI."""
      # Command bdist_msi does not support the library version, neither a date
      # as a version but if we suffix it with .1 everything is fine.
      self.distribution.metadata.version += '.1'

      bdist_msi.run(self)


if not bdist_rpm:
  BdistRPMCommand = None
else:
  class BdistRPMCommand(bdist_rpm):
    """Custom handler for the bdist_rpm command."""

    def _make_spec_file(self):
      """Generates the text of an RPM spec file.

      Returns:
        list[str]: lines of the RPM spec file.
      """
      # Note that bdist_rpm can be an old style class.
      if issubclass(BdistRPMCommand, object):
        spec_file = super(BdistRPMCommand, self)._make_spec_file()
      else:
        spec_file = bdist_rpm._make_spec_file(self)

      if sys.version_info[0] < 3:
        python_package = 'python'
      else:
        python_package = 'python3'

      description = []
      summary = ''
      in_description = False

      python_spec_file = []
      for line in iter(spec_file):
        if line.startswith('Summary: '):
          summary = line

        elif line.startswith('BuildRequires: '):
          line = 'BuildRequires: {0:s}-setuptools'.format(python_package)

        elif line.startswith('Requires: '):
          if python_package == 'python3':
            line = line.replace('python', 'python3')

        elif line.startswith('%description'):
          in_description = True

        elif line.startswith('%files'):
          # Cannot use %{_libdir} here since it can expand to "lib64".
          lines = [
              '%files',
              '%defattr(644,root,root,755)',
              '%doc ACKNOWLEDGEMENTS AUTHORS LICENSE README',
              '%{_prefix}/lib/python*/site-packages/acstore/*.py',
              '%{_prefix}/lib/python*/site-packages/acstore*.egg-info/*',
              '%exclude %{_prefix}/lib/python*/site-packages/acstore/*.pyc',
              '%exclude %{_prefix}/lib/python*/site-packages/acstore/*.pyo',
              ('%exclude %{_prefix}/lib/python*/site-packages/acstore/'
               '__pycache__/*')]

          python_spec_file.extend(lines)
          break

        elif line.startswith('%prep'):
          in_description = False

          python_spec_file.append(
              '%package -n {0:s}-%{{name}}'.format(python_package))
          python_spec_file.append('{0:s}'.format(summary))
          python_spec_file.append('')
          python_spec_file.append(
              '%description -n {0:s}-%{{name}}'.format(python_package))
          python_spec_file.extend(description)

        elif in_description:
          # Ignore leading white lines in the description.
          if not description and not line:
            continue

          description.append(line)

        python_spec_file.append(line)

      return python_spec_file


acstore_description = (
    'Attribute Container Storage (ACStore)')

acstore_long_description = (
    'ACStore, or Attribute Container Storage, provides a stand-alone '
    'implementation to read and write plaso storage files.')

setup(
    name='acstore',
    version=acstore.__version__,
    description=acstore_description,
    long_description=acstore_long_description,
    license='Apache License, Version 2.0',
    url='https://github.com/log2timeline/acstore',
    maintainer='dfDateTime development team',
    maintainer_email='log2timeline-dev@googlegroups.com',
    cmdclass={
        'bdist_msi': BdistMSICommand,
        'bdist_rpm': BdistRPMCommand},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    packages=find_packages('.', exclude=[
        'examples', 'tests', 'tests.*', 'utils']),
    package_dir={
        'acstore': 'acstore'
    },
    data_files=[
        ('share/doc/acstore', [
            'ACKNOWLEDGEMENTS', 'AUTHORS', 'LICENSE', 'README']),
    ],
)
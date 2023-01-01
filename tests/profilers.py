#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the profiler classes."""

import time
import unittest

from acstore import profilers

from tests import test_lib


class CPUTimeMeasurementTest(test_lib.BaseTestCase):
  """Tests for the CPU time measurement."""

  def testSampleStartStop(self):
    """Tests the SampleStart and SampleStop functions."""
    cpu_measurement = profilers.CPUTimeMeasurement()
    cpu_measurement.SampleStart()
    cpu_measurement.SampleStop()


class StorageProfilerTest(test_lib.BaseTestCase):
  """Tests for the storage profiler."""

  # pylint: disable=protected-access

  def testIsSupported(self):
    """Tests the IsSupported function."""
    self.assertTrue(profilers.StorageProfiler.IsSupported())

  def testStartStop(self):
    """Tests the Start and Stop functions."""
    with test_lib.TempDirectory() as temp_directory:
      test_profiler = profilers.StorageProfiler(
          'test', temp_directory)

      test_profiler._FILENAME_PREFIX = 'test'
      test_profiler._FILE_HEADER = 'test'

      test_profiler.Start()

      test_profiler.Stop()

  def testSample(self):
    """Tests the Sample function."""
    with test_lib.TempDirectory() as temp_directory:
      test_profiler = profilers.StorageProfiler(
          'test', temp_directory)

      test_profiler.Start()

      for _ in range(5):
        test_profiler.StartTiming('test_profile')
        time.sleep(0.01)
        test_profiler.StopTiming('test_profile')
        test_profiler.Sample('test_profile', 'read', 'test', 1024, 128)

      test_profiler.Stop()


if __name__ == '__main__':
  unittest.main()

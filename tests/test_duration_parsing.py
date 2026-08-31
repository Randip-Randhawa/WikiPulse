"""Tests for processing.streaming_job._duration_to_minutes, the small
parser that turns Spark-style duration strings ('5 minutes') into a
numeric minute value used for edit-velocity calculations.

Note: importing processing.streaming_job requires pyspark to be installed
(see requirements.txt) but does not require a running Spark/JVM session,
since only the module-level helper function is exercised here.
"""

from __future__ import annotations

import pytest

from processing.streaming_job import _duration_to_minutes


class TestDurationToMinutes:
    def test_minutes(self):
        assert _duration_to_minutes("5 minutes") == 5.0

    def test_single_minute(self):
        assert _duration_to_minutes("1 minute") == 1.0

    def test_seconds_converted_to_fractional_minutes(self):
        assert _duration_to_minutes("30 seconds") == pytest.approx(0.5)

    def test_hours_converted_to_minutes(self):
        assert _duration_to_minutes("2 hours") == 120.0

    def test_decimal_value(self):
        assert _duration_to_minutes("1.5 minutes") == 1.5

    def test_extra_whitespace_is_tolerated(self):
        assert _duration_to_minutes("  5   minutes  ") == 5.0

    def test_unrecognized_format_raises_value_error(self):
        with pytest.raises(ValueError):
            _duration_to_minutes("garbage")

    def test_unsupported_unit_raises_value_error(self):
        with pytest.raises(ValueError):
            _duration_to_minutes("5 fortnights")

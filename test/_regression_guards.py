"""Compatibility shims for branches that still import former guards.

The dialect implementations required by the affected tests are now present,
so these helpers intentionally perform no pytest control flow.  They are kept
only to make overlaying this correction onto an older working tree safe.
"""

from __future__ import annotations


def xfail_if_logical_reflected_names_are_unavailable() -> None:
    return None


def xfail_if_arithmetic_default_validation_is_unavailable() -> None:
    return None


def xfail_if_ondelete_validation_is_unavailable() -> None:
    return None

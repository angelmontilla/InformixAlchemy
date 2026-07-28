from __future__ import annotations

from pathlib import Path
import xml.etree.ElementTree as ET

import pytest

from run_sql_expression_certification import _assert_junit


def _write_junit(
    path: Path,
    *,
    passed: int = 0,
    failed: int = 0,
    errors: int = 0,
    skipped: int = 0,
) -> None:
    root = ET.Element("testsuite")

    for index in range(passed):
        ET.SubElement(root, "testcase", name=f"passed_{index}")

    for index in range(failed):
        case = ET.SubElement(root, "testcase", name=f"failed_{index}")
        ET.SubElement(case, "failure")

    for index in range(errors):
        case = ET.SubElement(root, "testcase", name=f"error_{index}")
        ET.SubElement(case, "error")

    for index in range(skipped):
        case = ET.SubElement(root, "testcase", name=f"skipped_{index}")
        ET.SubElement(case, "skipped")

    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def test_certification_junit_accepts_exact_success(tmp_path):
    report = tmp_path / "success.xml"
    _write_junit(report, passed=15)

    _assert_junit(report, expected_tests=15)


@pytest.mark.parametrize(
    ("result_kind", "kwargs"),
    [
        ("failure", {"passed": 14, "failed": 1}),
        ("error", {"passed": 14, "errors": 1}),
        ("skip", {"passed": 14, "skipped": 1}),
    ],
)
def test_certification_junit_rejects_nonpassing_cases(
    tmp_path,
    result_kind,
    kwargs,
):
    report = tmp_path / f"{result_kind}.xml"
    _write_junit(report, **kwargs)

    with pytest.raises(SystemExit, match="Certification failed"):
        _assert_junit(report, expected_tests=15)


def test_certification_junit_rejects_missing_cases(tmp_path):
    report = tmp_path / "incomplete.xml"
    _write_junit(report, passed=14)

    with pytest.raises(SystemExit, match="Expected 15 certified tests"):
        _assert_junit(report, expected_tests=15)

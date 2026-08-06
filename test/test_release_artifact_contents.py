from __future__ import annotations

import zipfile

from tools.verify_release_artifacts import verify_artifact


def test_wheel_structure_accepts_package_and_dist_info(tmp_path):
    wheel = tmp_path / "ifxalchemy-1.2.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("IfxAlchemy/__init__.py", "")
        archive.writestr("ifxalchemy-1.2.0.dist-info/METADATA", "")

    assert verify_artifact(wheel) == []


def test_wheel_structure_rejects_tests_and_environment_files(tmp_path):
    wheel = tmp_path / "ifxalchemy-1.2.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("IfxAlchemy/__init__.py", "")
        archive.writestr("test/test_example.py", "")
        archive.writestr(".env.informix", "PASSWORD=secret")

    issues = verify_artifact(wheel)

    assert any("forbidden path" in issue for issue in issues)
    assert any("forbidden file" in issue for issue in issues)

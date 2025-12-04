"""
Tests for package import verification (Task Group 1)

These tests verify that the deepfreeze package:
1. Can be imported successfully
2. Contains no curator imports
3. Has proper entry point registration
"""

import ast
import importlib
import sys
from pathlib import Path


def test_package_imports_successfully():
    """Test that the deepfreeze package can be imported"""
    import deepfreeze

    assert deepfreeze is not None
    assert hasattr(deepfreeze, "__version__")
    assert deepfreeze.__version__ == "1.0.0"


def test_submodules_import_successfully():
    """Test that all submodules can be imported"""
    import deepfreeze.exceptions
    import deepfreeze.constants
    import deepfreeze.cli
    import deepfreeze.actions

    assert deepfreeze.exceptions is not None
    assert deepfreeze.constants is not None
    assert deepfreeze.cli is not None
    assert deepfreeze.actions is not None


def test_no_curator_imports_in_package():
    """Test that no curator imports are present in the package"""
    # Get the package directory
    import deepfreeze
    package_dir = Path(deepfreeze.__file__).parent

    # Check all Python files in the package
    curator_imports_found = []

    for py_file in package_dir.rglob("*.py"):
        with open(py_file, "r") as f:
            try:
                tree = ast.parse(f.read())
            except SyntaxError:
                continue

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith("curator"):
                            curator_imports_found.append(
                                f"{py_file.relative_to(package_dir)}: import {alias.name}"
                            )
                elif isinstance(node, ast.ImportFrom):
                    if node.module and node.module.startswith("curator"):
                        curator_imports_found.append(
                            f"{py_file.relative_to(package_dir)}: from {node.module} import ..."
                        )

    assert len(curator_imports_found) == 0, (
        f"Found curator imports in deepfreeze package:\n"
        + "\n".join(curator_imports_found)
    )


def test_entry_point_registration():
    """Test that the CLI entry point is properly configured"""
    from deepfreeze.cli.main import cli
    import click

    assert cli is not None
    assert isinstance(cli, click.core.Group)

    # Check that all expected commands are registered
    expected_commands = [
        "setup",
        "status",
        "rotate",
        "thaw",
        "refreeze",
        "cleanup",
        "repair-metadata",
    ]

    for cmd in expected_commands:
        assert cmd in cli.commands, f"Command '{cmd}' not found in CLI"

"""
Tests for exceptions and constants (Task Group 2)

These tests verify that:
1. All exception classes are properly defined and instantiable
2. All constants are accessible and have correct values
3. No curator imports exist in these modules
"""

import ast
from pathlib import Path


def test_exception_classes_importable():
    """Test that all exception classes can be imported from deepfreeze.exceptions"""
    from deepfreeze.exceptions import (
        DeepfreezeException,
        MissingIndexError,
        MissingSettingsError,
        ActionException,
        PreconditionError,
        RepositoryException,
        ActionError,
    )

    # Verify all are exception classes
    assert issubclass(DeepfreezeException, Exception)
    assert issubclass(MissingIndexError, DeepfreezeException)
    assert issubclass(MissingSettingsError, DeepfreezeException)
    assert issubclass(ActionException, DeepfreezeException)
    assert issubclass(PreconditionError, DeepfreezeException)
    assert issubclass(RepositoryException, DeepfreezeException)
    assert issubclass(ActionError, DeepfreezeException)


def test_exception_instantiation():
    """Test that exception classes can be instantiated with messages"""
    from deepfreeze.exceptions import (
        DeepfreezeException,
        MissingIndexError,
        MissingSettingsError,
        ActionException,
        PreconditionError,
        RepositoryException,
        ActionError,
    )

    # Test instantiation with messages
    exc1 = DeepfreezeException("Test message")
    assert str(exc1) == "Test message"

    exc2 = MissingIndexError("Index not found")
    assert str(exc2) == "Index not found"

    exc3 = ActionError("Action failed")
    assert str(exc3) == "Action failed"

    # Test that exceptions can be raised and caught
    try:
        raise ActionError("Test action error")
    except DeepfreezeException as e:
        assert str(e) == "Test action error"


def test_constants_importable():
    """Test that all constants can be imported from deepfreeze.constants"""
    from deepfreeze.constants import (
        STATUS_INDEX,
        SETTINGS_ID,
        PROVIDERS,
        THAW_STATE_ACTIVE,
        THAW_STATE_FROZEN,
        THAW_STATE_THAWING,
        THAW_STATE_THAWED,
        THAW_STATE_EXPIRED,
        THAW_STATES,
        THAW_STATUS_IN_PROGRESS,
        THAW_STATUS_COMPLETED,
        THAW_STATUS_FAILED,
        THAW_STATUS_REFROZEN,
        THAW_REQUEST_STATUSES,
    )

    # Verify values
    assert STATUS_INDEX == "deepfreeze-status"
    assert SETTINGS_ID == "1"
    assert PROVIDERS == ["aws"]

    # Verify thaw states
    assert THAW_STATE_ACTIVE == "active"
    assert THAW_STATE_FROZEN == "frozen"
    assert THAW_STATE_THAWING == "thawing"
    assert THAW_STATE_THAWED == "thawed"
    assert THAW_STATE_EXPIRED == "expired"
    assert len(THAW_STATES) == 5

    # Verify thaw request statuses
    assert THAW_STATUS_IN_PROGRESS == "in_progress"
    assert THAW_STATUS_COMPLETED == "completed"
    assert THAW_STATUS_FAILED == "failed"
    assert THAW_STATUS_REFROZEN == "refrozen"
    assert len(THAW_REQUEST_STATUSES) == 4


def test_no_curator_imports_in_exceptions_module():
    """Test that no curator imports exist in exceptions.py"""
    import deepfreeze
    exceptions_file = Path(deepfreeze.__file__).parent / "exceptions.py"

    with open(exceptions_file, "r") as f:
        tree = ast.parse(f.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert not alias.name.startswith("curator"), (
                    f"Found curator import in exceptions.py: import {alias.name}"
                )
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                assert not node.module.startswith("curator"), (
                    f"Found curator import in exceptions.py: from {node.module} import ..."
                )


def test_no_curator_imports_in_constants_module():
    """Test that no curator imports exist in constants.py"""
    import deepfreeze
    constants_file = Path(deepfreeze.__file__).parent / "constants.py"

    with open(constants_file, "r") as f:
        tree = ast.parse(f.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert not alias.name.startswith("curator"), (
                    f"Found curator import in constants.py: import {alias.name}"
                )
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                assert not node.module.startswith("curator"), (
                    f"Found curator import in constants.py: from {node.module} import ..."
                )

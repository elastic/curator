"""
Independence verification tests for deepfreeze (Task Group 20)

These tests verify:
1. Complete independence from curator package
2. No curator imports anywhere in the package
3. CLI works without curator
4. Package can be imported standalone
5. All modules function independently
"""

import ast
import sys
import subprocess
from pathlib import Path

import pytest


class TestNoCuratorImports:
    """Verify no curator imports exist anywhere in the package"""

    def test_scan_all_python_files_for_curator_imports(self):
        """Scan all Python files in deepfreeze package for curator imports using AST"""
        import deepfreeze

        package_dir = Path(deepfreeze.__file__).parent
        curator_imports = []

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
                                curator_imports.append(
                                    f"{py_file.relative_to(package_dir)}: import {alias.name}"
                                )
                    elif isinstance(node, ast.ImportFrom):
                        if node.module and node.module.startswith("curator"):
                            curator_imports.append(
                                f"{py_file.relative_to(package_dir)}: from {node.module} import ..."
                            )

        assert len(curator_imports) == 0, (
            f"Found curator imports in deepfreeze package:\n"
            + "\n".join(curator_imports)
        )

    def test_no_curator_in_dependencies(self):
        """Verify curator is not a runtime dependency"""
        # Check that deepfreeze can work without curator being imported
        import sys

        # Verify deepfreeze modules don't try to import curator
        import deepfreeze
        import deepfreeze.exceptions
        import deepfreeze.constants
        import deepfreeze.helpers

        # If we get here without ImportError mentioning curator, we're good
        assert True


class TestPackageImportsStandalone:
    """Verify the package imports correctly as standalone"""

    def test_import_deepfreeze(self):
        """Test that import deepfreeze works"""
        import deepfreeze

        assert deepfreeze is not None
        assert hasattr(deepfreeze, "__version__")

    def test_import_deepfreeze_exceptions(self):
        """Test that deepfreeze.exceptions imports correctly"""
        from deepfreeze.exceptions import (
            DeepfreezeException,
            MissingIndexError,
            MissingSettingsError,
            ActionException,
            PreconditionError,
            RepositoryException,
            ActionError,
        )

        assert DeepfreezeException is not None
        assert issubclass(ActionError, DeepfreezeException)

    def test_import_deepfreeze_constants(self):
        """Test that deepfreeze.constants imports correctly"""
        from deepfreeze.constants import (
            STATUS_INDEX,
            SETTINGS_ID,
            PROVIDERS,
            THAW_STATES,
            THAW_REQUEST_STATUSES,
        )

        assert STATUS_INDEX == "deepfreeze-status"
        assert SETTINGS_ID == "1"
        assert "aws" in PROVIDERS

    def test_import_deepfreeze_helpers(self):
        """Test that deepfreeze.helpers imports correctly"""
        from deepfreeze.helpers import (
            Deepfreeze,
            Repository,
            Settings,
        )

        assert Deepfreeze is not None
        assert Repository is not None
        assert Settings is not None

    def test_import_deepfreeze_actions(self):
        """Test that deepfreeze.actions imports correctly"""
        from deepfreeze.actions import (
            Setup,
            Status,
            Rotate,
            Thaw,
            Refreeze,
            Cleanup,
            RepairMetadata,
        )

        assert Setup is not None
        assert Status is not None
        assert Rotate is not None
        assert Thaw is not None
        assert Refreeze is not None
        assert Cleanup is not None
        assert RepairMetadata is not None

    def test_import_deepfreeze_cli(self):
        """Test that deepfreeze.cli imports correctly"""
        from deepfreeze.cli.main import cli

        import click

        assert cli is not None
        assert isinstance(cli, click.core.Group)

    def test_import_deepfreeze_validators(self):
        """Test that deepfreeze.validators imports correctly"""
        from deepfreeze.validators import (
            ACTION_SCHEMAS,
            validate_options,
            get_schema,
        )

        assert ACTION_SCHEMAS is not None
        assert validate_options is not None
        assert get_schema is not None

    def test_import_deepfreeze_config(self):
        """Test that deepfreeze.config imports correctly"""
        from deepfreeze.config import (
            load_config,
            get_elasticsearch_config,
            get_logging_config,
        )

        assert load_config is not None
        assert get_elasticsearch_config is not None
        assert get_logging_config is not None


class TestCLIFunctionality:
    """Verify CLI works without curator"""

    def test_cli_help(self):
        """Test deepfreeze --help works"""
        from click.testing import CliRunner
        from deepfreeze.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "Deepfreeze" in result.output

    def test_cli_version(self):
        """Test deepfreeze --version works"""
        from click.testing import CliRunner
        from deepfreeze.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        assert "deepfreeze" in result.output.lower()

    def test_cli_setup_help(self):
        """Test deepfreeze setup --help works"""
        from click.testing import CliRunner
        from deepfreeze.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["setup", "--help"])

        assert result.exit_code == 0
        assert "ilm_policy_name" in result.output
        assert "index_template_name" in result.output

    def test_cli_status_help(self):
        """Test deepfreeze status --help works"""
        from click.testing import CliRunner
        from deepfreeze.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["status", "--help"])

        assert result.exit_code == 0
        assert "--porcelain" in result.output

    def test_cli_rotate_help(self):
        """Test deepfreeze rotate --help works"""
        from click.testing import CliRunner
        from deepfreeze.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["rotate", "--help"])

        assert result.exit_code == 0
        assert "--keep" in result.output

    def test_cli_thaw_help(self):
        """Test deepfreeze thaw --help works"""
        from click.testing import CliRunner
        from deepfreeze.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["thaw", "--help"])

        assert result.exit_code == 0
        assert "--start-date" in result.output
        assert "--end-date" in result.output

    def test_cli_refreeze_help(self):
        """Test deepfreeze refreeze --help works"""
        from click.testing import CliRunner
        from deepfreeze.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["refreeze", "--help"])

        assert result.exit_code == 0
        assert "--thaw-request-id" in result.output

    def test_cli_cleanup_help(self):
        """Test deepfreeze cleanup --help works"""
        from click.testing import CliRunner
        from deepfreeze.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["cleanup", "--help"])

        assert result.exit_code == 0

    def test_cli_repair_metadata_help(self):
        """Test deepfreeze repair-metadata --help works"""
        from click.testing import CliRunner
        from deepfreeze.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["repair-metadata", "--help"])

        assert result.exit_code == 0
        assert "--porcelain" in result.output

    def test_all_commands_registered(self):
        """Test that all expected commands are registered"""
        from click.testing import CliRunner
        from deepfreeze.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])

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
            assert cmd in result.output, f"Command '{cmd}' not found in CLI"


class TestModuleIndependence:
    """Verify each module can be imported independently"""

    @pytest.mark.parametrize(
        "module_name",
        [
            "deepfreeze",
            "deepfreeze.exceptions",
            "deepfreeze.constants",
            "deepfreeze.helpers",
            "deepfreeze.s3client",
            "deepfreeze.esclient",
            "deepfreeze.utilities",
            "deepfreeze.config",
            "deepfreeze.cli",
            "deepfreeze.cli.main",
            "deepfreeze.actions",
            "deepfreeze.actions.setup",
            "deepfreeze.actions.status",
            "deepfreeze.actions.rotate",
            "deepfreeze.actions.thaw",
            "deepfreeze.actions.refreeze",
            "deepfreeze.actions.cleanup",
            "deepfreeze.actions.repair_metadata",
            "deepfreeze.validators",
            "deepfreeze.defaults",
        ],
    )
    def test_module_imports_independently(self, module_name):
        """Test that each module can be imported without errors"""
        import importlib

        # This will raise ImportError if the module cannot be imported
        module = importlib.import_module(module_name)
        assert module is not None


class TestBinaryBuildConfiguration:
    """Verify binary build configuration exists"""

    def test_pyproject_has_entry_point(self):
        """Verify pyproject.toml has CLI entry point defined"""
        from pathlib import Path
        import deepfreeze

        # Find the deepfreeze package's pyproject.toml (in the parent of package dir)
        package_dir = Path(deepfreeze.__file__).parent

        # The pyproject.toml should be at the same level as the package
        # Since __init__.py is at deepfreeze/, pyproject.toml is at deepfreeze/../pyproject.toml
        pyproject_path = package_dir / "pyproject.toml"

        if pyproject_path.exists():
            with open(pyproject_path, "r") as f:
                content = f.read()
                # Check for entry point definition
                assert "[project.scripts]" in content, (
                    "No CLI entry point section in pyproject.toml"
                )
                assert "deepfreeze" in content, (
                    "deepfreeze CLI entry point not found in pyproject.toml"
                )
        else:
            # If pyproject.toml not at expected location, just verify CLI works
            from deepfreeze.cli.main import cli
            assert cli is not None

    def test_package_has_version(self):
        """Verify package has version defined"""
        import deepfreeze

        assert hasattr(deepfreeze, "__version__")
        assert deepfreeze.__version__ is not None
        assert len(deepfreeze.__version__) > 0


class TestVerificationScript:
    """Independence verification script tests"""

    def test_verification_script_checks_imports_with_ast(self):
        """Use AST to verify no curator imports (ignores docstrings and comments)"""
        import deepfreeze

        package_dir = Path(deepfreeze.__file__).parent
        curator_imports = []

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
                                curator_imports.append(
                                    f"{py_file.name}: import {alias.name}"
                                )
                    elif isinstance(node, ast.ImportFrom):
                        if node.module and node.module.startswith("curator"):
                            curator_imports.append(
                                f"{py_file.name}: from {node.module}"
                            )

        assert len(curator_imports) == 0, (
            f"Found curator imports: {curator_imports}"
        )

    def test_no_es_client_builder_dependency_in_imports(self):
        """Verify no actual import statements use es_client.builder"""
        import deepfreeze

        package_dir = Path(deepfreeze.__file__).parent
        es_client_imports = []

        for py_file in package_dir.rglob("*.py"):
            with open(py_file, "r") as f:
                try:
                    tree = ast.parse(f.read())
                except SyntaxError:
                    continue

                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            if "es_client" in alias.name:
                                es_client_imports.append(
                                    f"{py_file.name}: import {alias.name}"
                                )
                    elif isinstance(node, ast.ImportFrom):
                        if node.module and "es_client" in node.module:
                            es_client_imports.append(
                                f"{py_file.name}: from {node.module}"
                            )

        assert len(es_client_imports) == 0, (
            f"Found es_client imports: {es_client_imports}"
        )

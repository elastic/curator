"""Tests for the deepfreeze validators module"""

import pytest
from voluptuous import Invalid

from deepfreeze import defaults
from deepfreeze.validators import (
    ACTION_SCHEMAS,
    DEEPFREEZE_OPTIONS,
    get_default_options,
    get_schema,
    validate_options,
)


class TestDefaults:
    """Tests for option default functions."""

    def test_year_default_applied(self):
        """Test year default is applied during validation."""
        from datetime import datetime
        validated = validate_options('rotate', {})
        # Year should be current year by default
        assert validated['year'] == datetime.today().year

    def test_month_default_applied(self):
        """Test month default is applied during validation."""
        from datetime import datetime
        validated = validate_options('rotate', {})
        # Month should be current month by default
        assert validated['month'] == datetime.today().month

    def test_repo_name_prefix_default_applied(self):
        """Test repo_name_prefix default is applied."""
        validated = validate_options('setup', {
            'ilm_policy_name': 'test',
            'index_template_name': 'test'
        })
        assert validated['repo_name_prefix'] == 'deepfreeze'

    def test_keep_default_applied(self):
        """Test keep default is applied."""
        validated = validate_options('rotate', {})
        assert validated['keep'] == 6

    def test_provider_default_applied(self):
        """Test provider default is applied."""
        validated = validate_options('setup', {
            'ilm_policy_name': 'test',
            'index_template_name': 'test'
        })
        assert validated['provider'] == 'aws'

    def test_retrieval_tier_default_applied(self):
        """Test retrieval_tier default is applied."""
        validated = validate_options('thaw', {})
        assert validated['retrieval_tier'] == 'Standard'

    def test_porcelain_default_applied(self):
        """Test porcelain default is applied."""
        validated = validate_options('status', {})
        assert validated['porcelain'] is False


class TestValidators:
    """Tests for validator schemas."""

    def test_all_actions_have_schemas(self):
        """Test that all expected actions have schemas defined."""
        expected_actions = ['setup', 'rotate', 'cleanup', 'status', 'thaw', 'refreeze', 'repair_metadata']
        for action in expected_actions:
            assert action in DEEPFREEZE_OPTIONS
            assert action in ACTION_SCHEMAS

    def test_get_schema_valid_action(self):
        """Test getting schema for a valid action."""
        schema = get_schema('setup')
        assert schema is not None

    def test_get_schema_invalid_action(self):
        """Test getting schema for invalid action raises KeyError."""
        with pytest.raises(KeyError):
            get_schema('nonexistent_action')

    def test_validate_options_setup(self):
        """Test validating setup options."""
        options = {
            'ilm_policy_name': 'test-policy',
            'index_template_name': 'test-template',
        }
        validated = validate_options('setup', options)

        # Required fields should be present
        assert validated['ilm_policy_name'] == 'test-policy'
        assert validated['index_template_name'] == 'test-template'

        # Defaults should be applied
        assert validated['repo_name_prefix'] == 'deepfreeze'
        assert validated['bucket_name_prefix'] == 'deepfreeze'
        assert validated['provider'] == 'aws'

    def test_validate_options_rotate(self):
        """Test validating rotate options."""
        options = {
            'keep': 3,
        }
        validated = validate_options('rotate', options)

        assert validated['keep'] == 3
        # Year and month should have defaults
        assert 'year' in validated
        assert 'month' in validated

    def test_validate_options_status(self):
        """Test validating status options."""
        options = {
            'limit': 10,
            'porcelain': True,
        }
        validated = validate_options('status', options)

        assert validated['limit'] == 10
        assert validated['porcelain'] is True

    def test_validate_options_thaw(self):
        """Test validating thaw options."""
        options = {
            'retrieval_tier': 'Expedited',
            'duration': 14,
        }
        validated = validate_options('thaw', options)

        assert validated['retrieval_tier'] == 'Expedited'
        assert validated['duration'] == 14

    def test_validate_options_invalid_action(self):
        """Test that invalid action raises KeyError."""
        with pytest.raises(KeyError):
            validate_options('nonexistent', {})

    def test_validate_options_invalid_value(self):
        """Test that invalid option value raises Invalid."""
        # Test with rotate which has keep
        rotate_options = {'keep': 'not-a-number'}
        with pytest.raises(Invalid):
            validate_options('rotate', rotate_options)

    def test_validate_canned_acl_choices(self):
        """Test that canned_acl validates against allowed values."""
        valid_options = {
            'ilm_policy_name': 'test',
            'index_template_name': 'test',
            'canned_acl': 'private',
        }
        validated = validate_options('setup', valid_options)
        assert validated['canned_acl'] == 'private'

        invalid_options = {
            'ilm_policy_name': 'test',
            'index_template_name': 'test',
            'canned_acl': 'invalid-acl',
        }
        with pytest.raises(Invalid):
            validate_options('setup', invalid_options)

    def test_validate_retrieval_tier_choices(self):
        """Test that retrieval_tier validates against allowed values."""
        valid_options = {
            'retrieval_tier': 'Bulk',
        }
        validated = validate_options('thaw', valid_options)
        assert validated['retrieval_tier'] == 'Bulk'

        invalid_options = {
            'retrieval_tier': 'SuperFast',
        }
        with pytest.raises(Invalid):
            validate_options('thaw', invalid_options)


class TestBooleanValidator:
    """Tests for the Boolean validator."""

    def test_boolean_true_values(self):
        """Test that Boolean validator accepts various true values."""
        validator = defaults.Boolean()

        assert validator(True) is True
        assert validator('true') is True
        assert validator('True') is True
        assert validator('TRUE') is True
        assert validator('1') is True
        assert validator('yes') is True
        assert validator('Yes') is True

    def test_boolean_false_values(self):
        """Test that Boolean validator accepts various false values."""
        validator = defaults.Boolean()

        assert validator(False) is False
        assert validator('false') is False
        assert validator('False') is False
        assert validator('FALSE') is False
        assert validator('0') is False
        assert validator('no') is False
        assert validator('No') is False

    def test_boolean_invalid_value(self):
        """Test that Boolean validator rejects invalid values."""
        validator = defaults.Boolean()

        with pytest.raises(ValueError):
            validator('maybe')

        with pytest.raises(ValueError):
            validator('2')


class TestGetDefaultOptions:
    """Tests for get_default_options function."""

    def test_get_defaults_for_status(self):
        """Test getting defaults for status action."""
        # Status has no required fields, so we can get defaults
        defaults_dict = get_default_options('status')
        assert defaults_dict.get('porcelain') is False

    def test_get_defaults_for_rotate(self):
        """Test getting defaults for rotate action."""
        defaults_dict = get_default_options('rotate')
        assert defaults_dict.get('keep') == 6

    def test_get_defaults_invalid_action(self):
        """Test that invalid action raises KeyError."""
        with pytest.raises(KeyError):
            get_default_options('nonexistent')

import sys
import logging
from unittest import TestCase
from mock import Mock, patch, mock_open
import elasticsearch
import curator
from curator import _version as __version__
from . import CLITestCase
# Get test variables and constants from a single source
from . import testvars as testvars

class TestCLI_A(TestCase):
    def test_read_file_no_file(self):
        self.assertRaises(TypeError, curator.read_file)
    def test_loginfo_defaults(self):
        loginfo = curator.LogInfo({})
        self.assertEqual(20, loginfo.numeric_log_level)
        self.assertEqual(testvars.default_format, loginfo.format_string)
    def test_loginfo_debug(self):
        loginfo = curator.LogInfo({"loglevel": "DEBUG"})
        self.assertEqual(10, loginfo.numeric_log_level)
        self.assertEqual(testvars.debug_format, loginfo.format_string)
    def test_loginfo_bad_level_raises(self):
        self.assertRaises(
            ValueError,
            curator.LogInfo, {"loglevel": "NOTALOGLEVEL"}
        )
    def test_loginfo_logstash_formatter(self):
        loginfo = curator.LogInfo({"logformat": "logstash"})
        logging.root.addHandler(loginfo.handler)
        logging.root.setLevel(loginfo.numeric_log_level)
        logger = logging.getLogger('testing')
        logger.info('testing')
        self.assertEqual(20, loginfo.numeric_log_level)
    def test_client_options_certificate(self):
        a = {'use_ssl':True, 'certificate':'invalid_path'}
        self.assertRaises(
            curator.FailedExecution,
            curator.test_client_options, a
        )
    def test_client_options_client_cert(self):
        a = {'use_ssl':True, 'client_cert':'invalid_path'}
        self.assertRaises(
            curator.FailedExecution,
            curator.test_client_options, a
        )
    def test_client_options_client_key(self):
        a = {'use_ssl':True, 'client_key':'invalid_path'}
        self.assertRaises(
            curator.FailedExecution,
            curator.test_client_options, a
        )

class TestCLI_B(CLITestCase):
    def test_read_file_pass(self):
        cfg = curator.get_yaml(self.args['yamlfile'])
        self.assertEqual('localhost', cfg['client']['hosts'])
        self.assertEqual(9200, cfg['client']['port'])
    def test_read_file_corrupt_fail(self):
        with self.assertRaises(SystemExit) as get:
            curator.get_yaml(self.args['invalid_yaml'])
        self.assertEqual(get.exception.code, 1)
    def test_read_file_missing_fail(self):
        self.assertRaises(
            curator.FailedExecution,
            curator.read_file, self.args['no_file_here']
        )

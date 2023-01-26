import logging
from unittest import TestCase
import curator
from . import CLITestCase
from . import testvars

class TestCLI_A(TestCase):
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

# class TestCLI_B(CLITestCase):
#     def test_read_file_pass(self):
#         cfg = curator.get_yaml(self.args['yamlfile'])
#         self.assertEqual('http://127.0.0.1:9200', cfg['client']['hosts'])
#     def test_read_file_corrupt_fail(self):
#         with self.assertRaises(SystemExit) as get:
#             curator.get_yaml(self.args['invalid_yaml'])
#         self.assertEqual(get.exception.code, 1)
#     def test_read_file_missing_fail(self):
#         self.assertRaises(
#             curator.FailedExecution,
#             curator.read_file, self.args['no_file_here']
#         )

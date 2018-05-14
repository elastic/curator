import elasticsearch
import curator
import os
import json
import string, random, tempfile
import click
from click import testing as clicktest
from mock import patch, Mock

from . import CuratorTestCase
from . import testvars as testvars

import logging
logger = logging.getLogger(__name__)

host, port = os.environ.get('TEST_ES_SERVER', 'localhost:9200').split(':')
port = int(port) if port else 9200

def random_envvar(size):
    return ''.join(
        random.SystemRandom().choice(
            string.ascii_uppercase + string.digits
        ) for _ in range(size)
    )

class TestEnvVars(CuratorTestCase):
    def test_present(self):
        evar = random_envvar(8)
        os.environ[evar] = "1234"
        dollar = '${' + evar + '}'
        self.write_config(
            self.args['configfile'],
            testvars.client_config_envvars.format(dollar, port, 30)
        )
        cfg = curator.get_yaml(self.args['configfile'])
        self.assertEqual(
            cfg['client']['hosts'],
            os.environ.get(evar)
        )
        del os.environ[evar]
    def test_not_present(self):
        evar = random_envvar(8)
        dollar = '${' + evar + '}'
        self.write_config(
            self.args['configfile'],
            testvars.client_config_envvars.format(dollar, port, 30)
        )
        cfg = curator.get_yaml(self.args['configfile'])
        self.assertIsNone(cfg['client']['hosts'])
    def test_not_present_with_default(self):
        evar = random_envvar(8)
        default = random_envvar(8)
        dollar = '${' + evar + ':' + default + '}'
        self.write_config(
            self.args['configfile'],
            testvars.client_config_envvars.format(dollar, port, 30)
        )
        cfg = curator.get_yaml(self.args['configfile'])
        self.assertEqual(
            cfg['client']['hosts'],
            default
        )
    def test_do_something_with_int_value(self):
        self.create_indices(10)
        evar = random_envvar(8)
        os.environ[evar] = "1234"
        dollar = '${' + evar + '}'
        self.write_config(
            self.args['configfile'],
            testvars.client_config_envvars.format(host, port, dollar)
        )
        cfg = curator.get_yaml(self.args['configfile'])
        self.assertEqual(
            cfg['client']['timeout'],
            os.environ.get(evar)
        )
        self.write_config(self.args['actionfile'],
            testvars.delete_proto.format(
                'age', 'name', 'older', '\'%Y.%m.%d\'', 'days', 5, ' ', ' ', ' '
            )
        )
        test = clicktest.CliRunner()
        _ = test.invoke(
                    curator.cli,
                    [
                        '--config', self.args['configfile'],
                        self.args['actionfile']
                    ],
                    )
        self.assertEquals(5, len(curator.get_indices(self.client)))
        del os.environ[evar]

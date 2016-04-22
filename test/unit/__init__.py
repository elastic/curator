import os
import shutil
import tempfile
import random
import string
from unittest import SkipTest, TestCase
from mock import Mock
from .testvars import *

class CLITestCase(TestCase):
    def setUp(self):
        super(CLITestCase, self).setUp()
        self.args = {}
        dirname = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        ymlname = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        badyaml = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        # This will create a psuedo-random temporary directory on the machine
        # which runs the unit tests, but NOT on the machine where elasticsearch
        # is running. This means tests may fail if run against remote instances
        # unless you explicitly set `self.args['location']` to a proper spot
        # on the target machine.
        self.args['tmpdir'] = tempfile.mkdtemp(suffix=dirname)
        if not os.path.exists(self.args['tmpdir']):
            os.makedirs(self.args['tmpdir'])
        self.args['yamlfile'] = os.path.join(self.args['tmpdir'], ymlname)
        self.args['invalid_yaml'] = os.path.join(self.args['tmpdir'], badyaml)
        self.args['no_file_here'] = os.path.join(self.args['tmpdir'], 'not_created')
        with open(self.args['yamlfile'], 'w') as f:
            f.write(testvars.yamlconfig)
        with open(self.args['invalid_yaml'], 'w') as f:
            f.write('gobbledeygook: @failhere\n')

    def tearDown(self):
        if os.path.exists(self.args['tmpdir']):
            shutil.rmtree(self.args['tmpdir'])

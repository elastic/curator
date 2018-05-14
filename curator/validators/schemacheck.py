from voluptuous import Schema
from curator.exceptions import ConfigurationError
import re
import logging

class SchemaCheck(object):
    def __init__(self, config, schema, test_what, location):
        """
        Validate ``config`` with the provided voluptuous ``schema``.
        ``test_what`` and ``location`` are for reporting the results, in case of
        failure.  If validation is successful, the method returns ``config`` as
        valid.

        :arg config: A configuration dictionary.
        :type config: dict
        :arg schema: A voluptuous schema definition
        :type schema: :class:`voluptuous.Schema`
        :arg test_what: which configuration block is being validated
        :type test_what: str
        :arg location: An string to report which configuration sub-block is
            being tested.
        :type location: str
        """
        self.loggit = logging.getLogger('curator.validators.SchemaCheck')
        # Set the Schema for validation...
        self.loggit.debug('Schema: {0}'.format(schema))
        self.loggit.debug('"{0}" config: {1}'.format(test_what, config))
        self.config = config
        self.schema = schema
        self.test_what = test_what
        self.location = location

    def __parse_error(self):
        """
        Report the error, and try to report the bad key or value as well.
        """
        def get_badvalue(data_string, data):
            elements = re.sub(r'[\'\]]', '', data_string).split('[')
            elements.pop(0) # Get rid of data as the first element
            value = None
            for k in elements:
                try:
                    key = int(k)
                except ValueError:
                    key = k
                if value == None:
                    value = data[key]
                    # if this fails, it's caught below
            return value
        try:
            self.badvalue = get_badvalue(str(self.error).split()[-1], self.config)
        except:
            self.badvalue = '(could not determine)'

    def result(self):
        try:
            return self.schema(self.config)
        except Exception as e:
            try:
                # pylint: disable=E1101
                self.error = e.errors[0]
            except:
                self.error = '{0}'.format(e)
            self.__parse_error()
            self.loggit.error('Schema error: {0}'.format(self.error))
            raise ConfigurationError(
                'Configuration: {0}: Location: {1}: Bad Value: "{2}", {3}. '
                'Check configuration file.'.format(
                    self.test_what, self.location, self.badvalue, self.error)
            )

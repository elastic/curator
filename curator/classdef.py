"""Other Classes"""

import logging
from es_client.exceptions import FailedValidation
from es_client.helpers.schemacheck import password_filter
from es_client.helpers.utils import get_yaml
from curator import IndexList, SnapshotList
from curator.actions import CLASS_MAP
from curator.exceptions import ConfigurationError
from curator.helpers.testers import validate_actions

# Let me tell you the story of the nearly wasted afternoon and the research that went
# into this seemingly simple work-around. Actually, no. It's even more wasted time
# writing that story here. Suffice to say that I couldn't use the CLASS_MAP with class
# objects to directly map them to class instances. The Wrapper class and the
# ActionDef.instantiate method do all of the work for me, allowing me to easily and
# cleanly pass *args and **kwargs to the individual action classes of CLASS_MAP.


class Wrapper:
    """Wrapper Class"""

    def __init__(self, cls):
        """Instantiate with passed Class (not instance or object)

        :param cls: A class (not an instance of the class)
        """
        #: The class itself (not an instance of it), passed from ``cls``
        self.class_object = cls
        #: An instance of :py:attr:`class_object`
        self.class_instance = None

    def set_instance(self, *args, **kwargs):
        """Set up :py:attr:`class_instance` from :py:attr:`class_object`"""
        self.class_instance = self.class_object(*args, **kwargs)

    def get_instance(self, *args, **kwargs):
        """Return the instance with ``*args`` and ``**kwargs``"""
        self.set_instance(*args, **kwargs)
        return self.class_instance


class ActionsFile:
    """Class to parse and verify entire actions file

    Individual actions are :py:class:`~.curator.classdef.ActionDef` objects
    """

    def __init__(self, action_file):
        self.logger = logging.getLogger(__name__)
        #: The full, validated configuration from ``action_file``.
        self.fullconfig = self.get_validated(action_file)
        self.logger.debug('Action Configuration: %s', password_filter(self.fullconfig))
        #: A dict of all actions in the provided configuration. Each original key name
        #: is preserved and the value is now an
        #: :py:class:`~.curator.classdef.ActionDef`, rather than a dict.
        self.actions = None
        self.set_actions(self.fullconfig['actions'])

    def get_validated(self, action_file):
        """
        :param action_file: The path to a valid YAML action configuration file
        :type action_file: str

        :returns: The result from passing ``action_file`` to
            :py:func:`~.curator.helpers.testers.validate_actions`
        """
        try:
            return validate_actions(get_yaml(action_file))
        except (FailedValidation, UnboundLocalError) as err:
            self.logger.critical('Configuration Error: %s', err)
            raise ConfigurationError from err

    def parse_actions(self, all_actions):
        """Parse the individual actions found in ``all_actions['actions']``

        :param all_actions: All actions, each its own dictionary behind a numeric key.
            Making the keys numeric guarantees that if they are sorted, they will
            always be executed in order.

        :type all_actions: dict

        :returns:
        :rtype: list of :py:class:`~.curator.classdef.ActionDef`
        """
        acts = {}
        for idx in all_actions.keys():
            acts[idx] = ActionDef(all_actions[idx])
        return acts

    def set_actions(self, all_actions):
        """Set the actions via :py:meth:`~.curator.classdef.ActionsFile.parse_actions`

        :param all_actions: All actions, each its own dictionary behind a numeric key.
            Making the keys numeric guarantees that if they are sorted, they will
            always be executed in order.
        :type all_actions: dict
        :rtype: None
        """
        self.actions = self.parse_actions(all_actions)


# In this case, I just don't care that pylint thinks I'm overdoing it with attributes
# pylint: disable=too-many-instance-attributes
class ActionDef:
    """Individual Action Definition Class

    Instances of this class represent an individual action from an action file.
    """

    def __init__(self, action_dict):
        #: The whole action dictionary
        self.action_dict = action_dict
        #: The action name
        self.action = None
        #: The action's class (Alias, Allocation, etc.)
        self.action_cls = None
        #: Only when action is alias will this be a :py:class:`~.curator.IndexList`
        self.alias_adds = None
        #: Only when action is alias will this be a :py:class:`~.curator.IndexList`
        self.alias_removes = None
        #: The list class, either :py:class:`~.curator.IndexList` or
        #: :py:class:`~.curator.SnapshotList`. Default is
        #: :py:class:`~.curator.IndexList`
        self.list_obj = Wrapper(IndexList)
        #: The action ``description``
        self.description = None
        #: The action ``options`` :py:class:`dict`
        self.options = {}
        #: The action ``filters`` :py:class:`list`
        self.filters = None
        #: The action option ``disable_action``
        self.disabled = None
        #: The action option ``continue_if_exception``
        self.cif = None
        #: The action option ``timeout_override``
        self.timeout_override = None
        #: The action option ``ignore_empty_list``
        self.iel = None
        #: The action option ``allow_ilm_indices``
        self.allow_ilm = None
        self.set_root_attrs()
        self.set_option_attrs()
        self.log_the_options()
        self.get_action_class()

    def instantiate(self, attribute, *args, **kwargs):
        """
        Convert ``attribute`` from being a :py:class:`~.curator.classdef.Wrapper` of a
        Class to an instantiated object of that Class.

        This is madness or genius. You decide. This entire method plus the
        :py:class:`~.curator.classdef.Wrapper` class came about because I couldn't
        cleanly instantiate a class variable into a class object. It works, and that's
        good enough for me.

        :param attribute: The `name` of an attribute that references a Wrapper class
            instance
        :type attribute: str
        """
        try:
            wrapper = getattr(self, attribute)
        except AttributeError as exc:
            raise AttributeError(
                f'Bad Attribute: {attribute}. Exception: {exc}'
            ) from exc
        setattr(self, attribute, self.get_obj_instance(wrapper, *args, **kwargs))

    def get_obj_instance(self, wrapper, *args, **kwargs):
        """Get the class instance wrapper identified by ``wrapper``
        Pass all other args and kwargs to the
        :py:meth:`~.curator.classdef.Wrapper.get_instance` method.

        :returns: An instance of the class that :py:class:`~.curator.classdef.Wrapper`
            is wrapping
        """
        if not isinstance(wrapper, Wrapper):
            raise ConfigurationError(
                f'{__name__} was passed wrapper which was of type {type(wrapper)}'
            )
        return wrapper.get_instance(*args, **kwargs)

    def set_alias_extras(self):
        """Populate the :py:attr:`alias_adds` and :py:attr:`alias_removes` attributes"""
        self.alias_adds = Wrapper(IndexList)
        self.alias_removes = Wrapper(IndexList)

    def get_action_class(self):
        """Get the action class from :py:const:`~.curator.actions.CLASS_MAP`

        Do extra setup when action is ``alias``

        Set :py:attr:`list_obj` to :py:class:`~.curator.SnapshotList` when
        :py:attr:`~.curator.classdef.ActionDef.action` is ``delete_snapshots`` or
        ``restore``
        """

        self.action_cls = Wrapper(CLASS_MAP[self.action])
        if self.action == 'alias':
            self.set_alias_extras()
        if self.action in ['delete_snapshots', 'restore']:
            self.list_obj = Wrapper(SnapshotList)

    def set_option_attrs(self):
        """
        Iteratively get the keys and values from
        :py:attr:`~.curator.classdef.ActionDef.options` and set the attributes
        """
        attmap = {
            'disable_action': 'disabled',
            'continue_if_exception': 'cif',
            'ignore_empty_list': 'iel',
            'allow_ilm_indices': 'allow_ilm',
            'timeout_override': 'timeout_override',
        }
        for key in self.action_dict['options']:
            if key in attmap:
                setattr(self, attmap[key], self.action_dict['options'][key])
            else:
                self.options[key] = self.action_dict['options'][key]

    def set_root_attrs(self):
        """
        Iteratively get the keys and values from
        :py:attr:`~.curator.classdef.ActionDef.action_dict` and set the attributes
        """
        for key, value in self.action_dict.items():
            # Gonna grab options in get_option_attrs()
            if key == 'options':
                continue
            if value is not None:
                setattr(self, key, value)

    def log_the_options(self):
        """Log options at initialization time"""
        logger = logging.getLogger('curator.cli.ActionDef')
        msg = (
            f'For action {self.action}: disable_action={self.disabled}'
            f'continue_if_exception={self.cif}, '
            f'timeout_override={self.timeout_override}, '
            f'ignore_empty_list={self.iel}, allow_ilm_indices={self.allow_ilm}'
        )
        logger.debug(msg)
        if self.allow_ilm:
            logger.warning('Permitting operation on indices with an ILM policy')

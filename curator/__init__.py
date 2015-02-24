import logging
import elasticsearch

__version__ = '3.0.0-dev'

logger = logging.getLogger(__name__)

DATE_REGEX = {
    'Y' : '4',
    'y' : '2',
    'm' : '2',
    'W' : '2',
    'U' : '2',
    'd' : '2',
    'H' : '2',
}

from .utils import *
from .filter import *
from .alias import *
from .allocate import *
from .bloom import *
from .close import *
from .delete import *
from .optimize import *
from .replicas import *
from .snapshot import *
# from .curator import *

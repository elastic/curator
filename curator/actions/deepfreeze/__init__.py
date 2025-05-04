"""Deepfreeze actions module"""

from .constants import PROVIDERS, SETTINGS_ID, STATUS_INDEX
from .exceptions import (
    ActionException,
    DeepfreezeException,
    MissingIndexError,
    MissingSettingsError,
)
from .helpers import Deepfreeze, Repository, Settings
from .rotate import Rotate
from .setup import Setup
from .status import Status
from .utilities import (
    create_repo,
    decode_date,
    ensure_settings_index,
    get_all_indices_in_repo,
    get_all_repos,
    get_matching_repo_names,
    get_matching_repos,
    get_next_suffix,
    get_settings,
    get_timestamp_range,
    push_to_glacier,
    save_settings,
    unmount_repo,
)

CLASS_MAP = {
    "deepfreeze": Deepfreeze,
    "repository": Repository,
    "settings": Settings,
    "setup": Setup,
    "rotate": Rotate,
    "status": Status,
}

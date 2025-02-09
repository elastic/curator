"""Deepfreeze actions module"""

from .constants import PROVIDERS, SETTINGS_ID, STATUS_INDEX
from .helpers import Deepfreeze, Repository, Settings, ThawedRepo, ThawSet
from .refreeze import Refreeze
from .remount import Remount
from .rotate import Rotate
from .setup import Setup
from .status import Status
from .thaw import Thaw
from .utilities import (
    check_is_s3_thawed,
    check_restore_status,
    create_repo,
    decode_date,
    ensure_settings_index,
    get_all_indices_in_repo,
    get_matching_repo_names,
    get_matching_repos,
    get_next_suffix,
    get_settings,
    get_thawset,
    get_timestamp_range,
    get_unmounted_repos,
    push_to_glacier,
    save_settings,
    thaw_repo,
    unmount_repo,
    wait_for_s3_restore,
)

CLASS_MAP = {
    "deepfreeze": Deepfreeze,
    "repository": Repository,
    "settings": Settings,
    "thawedrepo": ThawedRepo,
    "thawset": ThawSet,
    "setup": Setup,
    "rotate": Rotate,
    "thaw": Thaw,
    "remount": Remount,
    "refreeze": Refreeze,
    "status": Status,
}

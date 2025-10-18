"""Deepfreeze actions module"""

from .constants import PROVIDERS, SETTINGS_ID, STATUS_INDEX
from .exceptions import (
    ActionException,
    DeepfreezeException,
    MissingIndexError,
    MissingSettingsError,
)
from .cleanup import Cleanup
from .helpers import Deepfreeze, Repository, Settings
from .refreeze import Refreeze
from .rotate import Rotate
from .setup import Setup
from .status import Status
from .thaw import Thaw
from .utilities import (
    check_restore_status,
    create_repo,
    decode_date,
    ensure_settings_index,
    find_repos_by_date_range,
    get_all_indices_in_repo,
    get_all_repos,
    get_matching_repo_names,
    get_matching_repos,
    get_next_suffix,
    get_repositories_by_names,
    get_settings,
    get_thaw_request,
    get_timestamp_range,
    list_thaw_requests,
    mount_repo,
    push_to_glacier,
    save_settings,
    save_thaw_request,
    unmount_repo,
    update_repository_date_range,
)

CLASS_MAP = {
    "cleanup": Cleanup,
    "deepfreeze": Deepfreeze,
    "refreeze": Refreeze,
    "repository": Repository,
    "settings": Settings,
    "setup": Setup,
    "rotate": Rotate,
    "status": Status,
    "thaw": Thaw,
}

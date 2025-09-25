import os
from enum import Enum
from typing import TypedDict, Optional

import singer


logger = singer.get_logger()


class ReplicationMethod(Enum):
    APPEND = "append"
    TRUNCATE = "truncate"
    INCREMENTAL = "incremental"


class ForceField(TypedDict, total=False):
    type: str
    mode: Optional[str]
    description: Optional[str]


class TableConfig(TypedDict, total=False):
    partition_field: Optional[str]
    cluster_fields: list[str]
    replication_method: Optional[ReplicationMethod]
    force_fields: dict[str, ForceField]


class TablesConfig(TypedDict):
    streams: dict[str, TableConfig]


class TargetConfig(TypedDict, total=False):
    project_id: str
    dataset_id: str
    location: Optional[str]
    replication_method: Optional[ReplicationMethod]
    truncate_on_full_sync: Optional[bool]
    table_prefix: Optional[str]
    table_suffix: Optional[str]
    validate_records: Optional[bool]
    add_metadata_columns: Optional[bool]
    force_alphanumeric_table_names: Optional[bool]
    merge_state_messages: Optional[bool]


def determine_replication_method(
    replication_method: Optional[str], truncate_on_full_sync: Optional[bool]
) -> ReplicationMethod:
    """Determine replication method using the exact same logic as before."""
    if replication_method == "append":
        if truncate_on_full_sync and os.environ.get("SYNC_TYPE") == "full_sync":
            return ReplicationMethod.TRUNCATE
        else:
            return ReplicationMethod.APPEND
    elif replication_method == "truncate":
        return ReplicationMethod.TRUNCATE
    elif replication_method == "incremental":
        return ReplicationMethod.INCREMENTAL
    else:
        if replication_method is not None:
            logger.warning(
                f"Unrecognized replication_method '{replication_method}', defaulting to 'append'"
            )
        return ReplicationMethod.APPEND


def apply_config_defaults(config: TargetConfig) -> TargetConfig:
    """Apply default values to config using the exact same logic as before."""
    # Set location default
    if "location" not in config:
        config["location"] = "US"

    # Determine replication method using the extracted function
    config["replication_method"] = determine_replication_method(
        config.get("replication_method"), config.get("truncate_on_full_sync")
    )

    # Set other defaults
    if "table_prefix" not in config:
        config["table_prefix"] = ""
    if "table_suffix" not in config:
        config["table_suffix"] = ""
    if "validate_records" not in config:
        config["validate_records"] = True
    if "add_metadata_columns" not in config:
        config["add_metadata_columns"] = True
    if "force_alphanumeric_table_names" not in config:
        config["force_alphanumeric_table_names"] = False
    if "merge_state_messages" not in config:
        config["merge_state_messages"] = True

    return config

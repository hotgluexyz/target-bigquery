import os
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator
import singer


logger = singer.get_logger()


class ReplicationMethod(str, Enum):
    APPEND = "append"
    TRUNCATE = "truncate"
    INCREMENTAL = "incremental"


class ForceField(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str
    mode: Optional[str] = None
    description: Optional[str] = None


class TableConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    partition_field: Optional[str] = None
    cluster_fields: list[str] = Field(default_factory=list)
    replication_method: Optional[ReplicationMethod] = None
    force_fields: dict[str, ForceField] = Field(default_factory=dict)


class TablesConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    streams: dict[str, TableConfig] = Field(default_factory=dict)


class TargetConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    # Required fields
    project_id: str
    dataset_id: str

    # Fields with defaults
    location: str = "US"
    replication_method: Optional[ReplicationMethod] = None
    truncate_on_full_sync: Optional[bool] = None
    table_prefix: str = ""
    table_suffix: str = ""
    validate_records: bool = True
    add_metadata_columns: bool = True
    force_alphanumeric_table_names: bool = False
    merge_state_messages: bool = True
    table_config: Optional[str] = None

    @model_validator(mode="after")
    def determine_replication_method(self):
        """Determine replication method using the exact same logic as before."""
        if self.replication_method is None:
            replication_method = None
        else:
            replication_method = self.replication_method.value

        if replication_method == "append":
            if (
                self.truncate_on_full_sync
                and os.environ.get("SYNC_TYPE") == "full_sync"
            ):
                self.replication_method = ReplicationMethod.TRUNCATE
            else:
                self.replication_method = ReplicationMethod.APPEND
        elif replication_method == "truncate":
            self.replication_method = ReplicationMethod.TRUNCATE
        elif replication_method == "incremental":
            self.replication_method = ReplicationMethod.INCREMENTAL
        else:
            if replication_method is not None:
                logger.warning(
                    f"Unrecognized replication_method '{replication_method}', defaulting to 'append'"
                )
            self.replication_method = ReplicationMethod.APPEND

        return self

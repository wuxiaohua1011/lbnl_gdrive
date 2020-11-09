from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional


class GDriveLog(BaseModel):
    GDriveID: str = Field(..., title="Google Drive ID")
    path: str = Field(..., title="Path for the file",
                      description="Should reflect both local disk space AND google drive path")
    last_updated: datetime = Field(default=datetime.now())
    created_at: datetime = Field(default=datetime.now())
    block_name: str = Field(..., title="Block that this record belongs to")
    is_block: bool = Field(default=True, title="Is this a block record or not")
    launcher_paths: Optional[list] = Field(default=[],
                                           description="If it is a block, then it will be populated with the launchers "
                                                       "that belongs to this block. Otherwise, it will be an empty list")
    launcher_name: Optional[str] = Field(default=None, description="If it is a launcher, then this field will be "
                                                                   "popuulated, otherwise, it will be empty")
    # task_id: Optional[str] = Field(default=None, description="if this is a launcher, then this field is populated")


class TaskRecord(BaseModel):
    dir_name: str = Field(..., title="Path name in the tasks collection")
    task_id: str = Field(...)
    last_updated: datetime = Field(...)
    dir_name_full: str = Field(...)

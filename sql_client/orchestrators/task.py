"""
`DataTask` domain model
"""

from sqlalchemy.orm import registry

mapper_registry = registry()


@mapper_registry.mapped
class DataTask:
    """
    DataTask ORM class
    """
    __tablename__ = "data_task"

    task: str
    is_finished: bool
    is_successful: bool
    is_failed: bool
    is_cancelled: bool
    retry_count: int

    @property
    def is_retried(self):
        """
        Returns True if task is retried
        """
        return bool(self.retry_count)

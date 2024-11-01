from dataclasses import dataclass
from datetime import datetime

from utils.exceptions import (
    MessageComponentPropertyErr,
    MessageComponentTypeErr,
    MessageLogTypeErr,
)


@dataclass
class Component:
    id: str
    name: str


@dataclass
class User:
    user_id: str
    name: str
    email: str


@dataclass
class Status:
    code: int
    message: str


class AbstractLog:
    log_type: str
    component: Component
    user: User
    status: Status
    Message: dict
    created_on: datetime

    def validate_log_types(self):
        if self.log_type not in MESSAGE_LOG_TYPES:
            raise MessageLogTypeErr(self.log_type)

    def validate_component(self):
        # Invalid component type
        if not isinstance(self.component, Component):
            raise MessageComponentTypeErr(self.component, Component)
        # Empty component values
        if not self.component.id and not self.component.name:
            raise MessageComponentPropertyErr(self.component)


@dataclass
class AuditLog(AbstractLog):
    method: str


@dataclass
class DataLog(AbstractLog):
    action: str


@dataclass
class SystemLog(AbstractLog):
    level: str

# handlers/__init__.py
from . import common, registration, shifts, admin, chat, tasks

routers = [
    common.router,
    registration.router,
    shifts.router,
    admin.router,
    chat.router,
    tasks.router,
]

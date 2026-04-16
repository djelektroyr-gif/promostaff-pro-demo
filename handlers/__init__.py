# handlers/__init__.py
from . import common, registration, shifts, admin, chat, tasks, shift_center

routers = [
    common.router,
    registration.router,
    shifts.router,
    shift_center.router,
    admin.router,
    chat.router,
    tasks.router,
    common.fallback_router,
]

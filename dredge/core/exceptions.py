class UserError(Exception):
    """An actionable error that can be displayed without a traceback."""


class UserAbort(Exception):
    """An intentional cancellation of an interactive operation."""

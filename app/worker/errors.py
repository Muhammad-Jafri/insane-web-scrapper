class RetryableError(Exception):
    """Transient failure — job will be re-queued with backoff."""


class FatalError(Exception):
    """Permanent failure — job is marked DEAD, no retry."""

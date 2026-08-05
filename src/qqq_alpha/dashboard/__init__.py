"""Read-only admin dashboard.

Never a second source of truth: every view reads the same journal files and
the same memory database the live engine already writes. There is no
parallel state to drift out of sync, and nothing here can place, close, or
alter a trade — it can only show what happened and let the operator approve
or reject a proposed lesson, exactly like the Telegram command listener.
"""

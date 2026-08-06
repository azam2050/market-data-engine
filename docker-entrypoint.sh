#!/bin/sh
# Railway mounts the persistent volume at container start, owned by root —
# that overwrites the chown done at build time in the Dockerfile. Fix
# ownership here, as root, before dropping to the unprivileged engine user.
set -e
chown -R engine:engine /app/var
exec gosu engine "$@"

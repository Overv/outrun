"""Module defining various global constants."""

# outrun version
VERSION = "1.0.0"

# outrun protocol
# The major version must be identical on local and remote.
#
# Note that many changes, like improved prefetching rules, can be implemented without
# having to change the protocol version.
PROTOCOL_VERSION = "1.0.0"

# Special exit code for when outrun itself fails.
OUTRUN_ERROR_CODE = 254

# Application ID used to derive an app-specific machine identifier from /etc/machine-id.
# See http://man7.org/linux/man-pages/man3/sd_id128_get_machine_app_specific.3.html.
APP_ID = b"a4313318cef44e0ca7ecdca13fdc417a"

# Name of the FUSE file system
FILESYSTEM_NAME = "outrunfs"

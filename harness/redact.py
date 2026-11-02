"""Scrubs credentials from text before it is written to results or shown in a job log.

GitHub masks only the secrets it knows; vended storage credentials and tokens are minted at
run time, so error messages that quote a signed URL or a request header must pass through here.
"""

import re

# Each pattern captures the part to keep in group 1; everything matched after it is the secret.
_PATTERNS = [
    # Azure SAS signature and AWS presigned-URL parameters in any URL.
    re.compile(r"(?i)(\b(?:sig|x-amz-security-token|x-amz-signature|x-amz-credential)=)[^&\s\"'<>]+"),
    # Authorization headers.
    re.compile(r"(?i)(\b(?:bearer|basic)\s+)[A-Za-z0-9._~+/=-]{8,}"),
    # Catalog and storage properties that carry secrets, as key=value or "key": "value".
    re.compile(
        r"(?i)([\"']?\b(?:access_token|client_secret|credential|token|s3\.secret-access-key"
        r"|s3\.session-token|adls\.sas-token[\w.-]*)[\"']?\s*[:=]\s*[\"']?)[^\"'\s,}&]+"
    ),
]


def redact(text):
    for pattern in _PATTERNS:
        text = pattern.sub(r"\1REDACTED", text)
    return text

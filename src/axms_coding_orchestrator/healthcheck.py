"""Container health probe for the local coding runtime."""

from __future__ import annotations

import os
import sys
from urllib.error import URLError
from urllib.request import urlopen


def main() -> None:
    port = os.environ.get("AXMS_HEALTH_PORT", "8090")
    try:
        with urlopen(f"http://127.0.0.1:{port}/health/ready", timeout=2) as response:
            healthy = response.status == 200
    except (OSError, URLError, ValueError):
        healthy = False
    raise SystemExit(0 if healthy else 1)


if __name__ == "__main__":
    main()

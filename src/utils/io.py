import io
import sys


def setup_stdout_utf8() -> None:
    """Reconfigure stdout to UTF-8 on Windows where the default is cp1252."""
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

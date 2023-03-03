from pathlib import Path

def resolve_path(path):
    return Path(path).resolve().as_posix()
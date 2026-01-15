import os
from typing import List


def is_workspace_path(path: str) -> bool:
    if not isinstance(path, str):
        return False
    return path.startswith("/Workspace/") or path.startswith("/Repos/")


def list_yaml_files_recursive(base_path: str, dbutils=None) -> List[str]:
    files: List[str] = []
    try:
        if is_workspace_path(base_path):
            for root, _, filenames in os.walk(base_path):
                for name in filenames:
                    if name.endswith(".yaml"):
                        files.append(os.path.join(root, name))
            return files

        # Fallback to dbutils.fs.ls for DBFS and other schemes
        if dbutils is None:
            return files
        stack = [base_path]
        while stack:
            current = stack.pop()
            try:
                for item in dbutils.fs.ls(current):
                    if getattr(item, 'isDir', False):
                        stack.append(item.path)
                    elif str(item.name).endswith('.yaml'):
                        files.append(item.path)
            except Exception:
                # Ignore inaccessible branches
                continue
    except Exception:
        pass
    return files



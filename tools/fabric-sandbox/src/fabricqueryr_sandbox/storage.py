"""Bound visible sandbox Lakehouse storage without starting Spark or capacity."""

from __future__ import annotations

import os
from pathlib import Path

from azure.core.exceptions import ResourceNotFoundError

LAKEHOUSES = {"TestLakehouse", "TestLakehouseNoSchemas"}
DEFAULT_MAX_BYTES = 1024**3
DEFAULT_MAX_ENTRIES = 20_000


def relative_path(path):
    """Reject paths that could escape a selected item or follow encoded separators."""
    if not isinstance(path, str) or any(c in path for c in ("\\", "%", "?", "#")):
        raise ValueError("Unexpected OneLake path")
    if any(part in {"", ".", ".."} for part in path.split("/")):
        raise ValueError("Unexpected OneLake path")
    return path


def inside(path, parent):
    return path == parent or path.startswith(parent + "/")


def children(filesystem, parent):
    """List only immediate children; validate service paths before using them."""
    relative_path(parent)
    try:
        for entry in filesystem.get_paths(path=parent, recursive=False):
            path = relative_path(entry.name)
            if path.rsplit("/", 1)[0] != parent:
                raise ValueError("OneLake listing escaped its requested directory")
            yield entry
    except ResourceNotFoundError:
        return  # An interrupted first provision may not have made this directory.


def shortcut_paths(api, workspace_id, item_id):
    root = f"/workspaces/{workspace_id}/items/{item_id}/shortcuts"
    path, seen, result = root, set(), set()
    while path:
        if path in seen:
            raise RuntimeError("Fabric returned a repeated shortcut continuation URI")
        seen.add(path)
        payload = api.request("GET", path).json()
        for link in payload["value"]:
            result.add(relative_path(f"{link['path']}/{link['name']}"))
        path = payload.get("continuationUri")
    return result


def limit(name, default):
    value = int(os.environ.get(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def check_storage(api, service, workspace_id):
    """Report and enforce a combined budget for the two owned fixture Lakehouses.

    Includes live files and retained Delta data/log files, skips shortcut targets.
    This is not a billing meter: soft-deleted and service-managed storage is not
    exposed by this listing. Unknown local data counts but is never deleted.
    Caller must have verified workspace ownership and an active session.
    """
    max_bytes = limit("FABRIC_SANDBOX_MAX_STORAGE_BYTES", DEFAULT_MAX_BYTES)
    max_entries = limit("FABRIC_SANDBOX_MAX_STORAGE_ENTRIES", DEFAULT_MAX_ENTRIES)
    filesystem = service.get_file_system_client(workspace_id)
    total_bytes = entries = skipped = 0
    reports = []
    exceeded = False
    for item in api.list_items(workspace_id):
        if item["type"] != "Lakehouse" or item["displayName"] not in LAKEHOUSES:
            continue
        item_id = relative_path(item["id"])
        links = {f"{item_id}/{p}" for p in shortcut_paths(api, workspace_id, item_id)}
        pending = [f"{item_id}/Files", f"{item_id}/Tables"]
        size, files = 0, 0
        while pending and not exceeded:
            parent = pending.pop()
            if any(inside(parent, link) for link in links):
                skipped += 1
                continue
            for entry in children(filesystem, parent):
                entries += 1
                if entries > max_entries:
                    exceeded = True
                    break
                if any(inside(entry.name, link) for link in links):
                    skipped += 1
                    continue
                if entry.is_directory:
                    pending.append(entry.name)
                else:
                    length = entry.content_length
                    if (
                        not isinstance(length, int)
                        or isinstance(length, bool)
                        or length < 0
                    ):
                        raise ValueError("OneLake returned an invalid file size")
                    size += length
                    total_bytes += length
                    files += 1
                    if total_bytes > max_bytes:
                        exceeded = True
                        break
        reports.append(f"{item['displayName']}: {size:,} bytes in {files:,} files")
        if exceeded:
            break
    message = (
        f"Lakehouse storage for {workspace_id}: {total_bytes:,} bytes; "
        f"{entries:,} entries; {skipped} shortcut targets excluded.\n"
        + "\n".join(reports)
        + f"\nBudget: {max_bytes:,} bytes / {max_entries:,} entries per workspace.\n"
        + "Includes retained Delta files. Excludes soft-deleted data and other Fabric services.\n"
    )
    if exceeded:
        message += "Storage budget exceeded; scan stopped early. Review storage before continuing.\n"
    print(message, flush=True)
    if destination := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(destination).open("a", encoding="utf-8") as stream:
            stream.write("\n### Sandbox storage\n\n" + message.replace("\n", "\n\n"))
    if exceeded:
        raise RuntimeError("Sandbox Lakehouse storage budget exceeded")
    return {"bytes": total_bytes, "entries": entries, "shortcuts_skipped": skipped}

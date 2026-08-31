"""Command-line entry point for sandbox lifecycle operations."""

from __future__ import annotations

import argparse
from datetime import timedelta

from .cleanup import cleanup_ci_workspaces, remove_persistent_workspace
from .deploy import deploy
from .discover import discover, discover_onelake
from .seed import seed
from .settings import SandboxSettings


def doctor(settings: SandboxSettings) -> int:
    missing = settings.validate_local_paths()
    if missing:
        for path in missing:
            print(f"missing: {path}")
        return 1
    print(f"workspace definitions: {settings.workspace_definition_dir}")
    print(f"fixtures: {settings.fixture_dir}")
    print(f"manifest: {settings.manifest_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fabric-sandbox")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("doctor", help="validate local sandbox configuration")
    subparsers.add_parser("deploy", help="publish Fabric workspace items")
    subparsers.add_parser("seed", help="upload fixtures and run the seed notebook")
    discover_parser = subparsers.add_parser(
        "discover", help="write the R integration-test manifest"
    )
    discover_parser.add_argument(
        "--scope",
        choices=("all", "onelake"),
        default="all",
        help="discover all services or only OneLake Delta test items",
    )
    cleanup_parser = subparsers.add_parser(
        "cleanup",
        help="find CI workspaces left by interrupted integration runs",
    )
    cleanup_parser.add_argument(
        "--confirm",
        action="store_true",
        help="delete matching workspaces; without this flag, only list them",
    )
    cleanup_parser.add_argument(
        "--repository",
        help="repository owner/name; defaults to GITHUB_REPOSITORY",
    )
    cleanup_parser.add_argument(
        "--minimum-age-hours",
        type=float,
        default=6,
        help="delete only workspaces at least this old (default: 6)",
    )
    persistent_parser = subparsers.add_parser(
        "remove-persistent",
        help="find a persistent sandbox owned by this repository",
    )
    persistent_parser.add_argument(
        "--workspace-name",
        required=True,
        help="exact display name of the persistent workspace",
    )
    persistent_parser.add_argument(
        "--owner-id",
        required=True,
        help="exact owner object ID in the workspace marker",
    )
    persistent_parser.add_argument(
        "--managed-by",
        required=True,
        help="exact managing workflow in the workspace marker",
    )
    persistent_parser.add_argument(
        "--confirm",
        action="store_true",
        help="delete matching workspaces; without this flag, only list them",
    )
    persistent_parser.add_argument(
        "--repository",
        help="repository owner/name; defaults to GITHUB_REPOSITORY",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "cleanup":
        workspaces = cleanup_ci_workspaces(
            confirm=args.confirm,
            repository=args.repository,
            minimum_age=timedelta(hours=args.minimum_age_hours),
        )
        verb = "deleted" if args.confirm else "found"
        print(f"{verb} {len(workspaces)} CI sandbox workspace(s)")
        return 0
    if args.command == "remove-persistent":
        workspaces = remove_persistent_workspace(
            workspace_name=args.workspace_name,
            owner_id=args.owner_id,
            managed_by=args.managed_by,
            confirm=args.confirm,
            repository=args.repository,
        )
        verb = "deleted" if args.confirm else "found"
        print(f"{verb} {len(workspaces)} persistent sandbox workspace(s)")
        return 0
    settings = SandboxSettings.from_environment()
    if args.command == "doctor":
        return doctor(settings)
    if args.command == "deploy":
        deploy(settings)
        return 0
    if args.command == "seed":
        seed(settings)
        return 0
    if args.command == "discover":
        manifest = (
            discover_onelake(settings)
            if args.scope == "onelake"
            else discover(settings)
        )
        print(
            f"wrote manifest for {len(manifest.items)} items: "
            f"{settings.manifest_path}"
        )
        return 0
    raise AssertionError(f"unhandled command: {args.command}")

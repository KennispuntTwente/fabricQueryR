"""Command-line entry point for sandbox lifecycle operations."""

from __future__ import annotations

import argparse
from datetime import timedelta

from .cleanup import cleanup_ci_workspaces
from .deploy import deploy
from .discover import discover
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
    subparsers.add_parser("discover", help="write the R integration-test manifest")
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
    return parser


def main() -> int:
    args = build_parser().parse_args()
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
        manifest = discover(settings)
        print(
            f"wrote manifest for {len(manifest.items)} items: "
            f"{settings.manifest_path}"
        )
        return 0
    if args.command == "cleanup":
        workspaces = cleanup_ci_workspaces(
            confirm=args.confirm,
            repository=args.repository,
            minimum_age=timedelta(hours=args.minimum_age_hours),
        )
        verb = "deleted" if args.confirm else "found"
        print(f"{verb} {len(workspaces)} CI sandbox workspace(s)")
        return 0
    raise AssertionError(f"unhandled command: {args.command}")

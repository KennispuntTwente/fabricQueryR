"""Command-line entry point for sandbox lifecycle operations."""

from __future__ import annotations

import argparse

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
        print(f"wrote manifest for {len(manifest.items)} items: {settings.manifest_path}")
        return 0
    raise AssertionError(f"unhandled command: {args.command}")
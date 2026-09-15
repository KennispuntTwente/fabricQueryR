"""Track successful item publication independently of seeded data revisions."""

from hashlib import sha256
import json

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient

from .credentials import get_credential
from .fixture_revision import _fixture_input_bytes


def deployment_items(settings, *, scope="all"):
    if scope == "onelake":
        return []
    types = set(settings.item_types)
    if scope == "jobs":
        types &= {"Notebook", "DataPipeline", "SparkJobDefinition", "Environment"}
    return sorted(
        path.name for path in settings.workspace_definition_dir.iterdir()
        if path.is_dir() and path.name != "SeedFixtures.Notebook"
        and path.name.rsplit(".", 1)[-1] in types
    )


def deployment_revision(settings, item):
    root = settings.workspace_definition_dir
    files = [root / "parameter.yml"] + sorted(
        path for path in (root / item).rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
        and path.suffix != ".pyc"
    )
    digest = sha256()
    digest.update(json.dumps({
        "environment": settings.environment,
        "runtime": settings.spark_runtime_version,
        "lane": settings.spark_runtime_lane,
    }, sort_keys=True).encode("utf-8"))
    for path in files:
        digest.update(path.relative_to(root).as_posix().encode("utf-8") + b"\0")
        digest.update(_fixture_input_bytes(path) + b"\0")
    return digest.hexdigest()


def _service(service_client=None, credential=None):
    return service_client or DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=credential or get_credential(),
    )


def _marker(service, workspace_id, lakehouse_id, item):
    return service.get_file_system_client(workspace_id).get_file_client(
        f"{lakehouse_id}/Files/fabricqueryr-deployed-{item}.json"
    )


def record_deployments(
    settings, workspace_id, lakehouse_id, items=None,
    *, service_client=None, credential=None,
):
    """Record only definitions whose publication completed successfully."""
    service = _service(service_client, credential)
    selected = set(deployment_items(settings))
    if items is not None:
        selected &= set(items)
    for item in sorted(selected):
        payload = {"revision": deployment_revision(settings, item)}
        _marker(service, workspace_id, lakehouse_id, item).upload_data(
            json.dumps(payload, sort_keys=True).encode("utf-8"), overwrite=True,
        )


def verify_deployments(
    settings, workspace_id, lakehouse_id, *, service_client=None, scope="all",
):
    items = deployment_items(settings, scope=scope)
    if not items:
        return
    service = _service(service_client)
    stale = []
    for item in items:
        try:
            payload = _marker(service, workspace_id, lakehouse_id, item)
            contract = json.loads(payload.download_file().readall())
        except (ResourceNotFoundError, ValueError, UnicodeDecodeError):
            contract = None
        if not isinstance(contract, dict) or contract.get("revision") != deployment_revision(settings, item):
            stale.append(item)
    if stale:
        raise RuntimeError(
            "Fabric item definitions need deployment: " + ", ".join(stale)
            + ". Deploy these items before running integration tests; reseeding is not required."
        )

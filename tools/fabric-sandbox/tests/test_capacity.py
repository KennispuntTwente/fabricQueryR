import httpx
import pytest
from azure.core.credentials import AccessToken
from fabricqueryr_sandbox.capacity import F2Capacity
from fabricqueryr_sandbox.fabric_api import FabricApi

RESOURCE = "/subscriptions/11111111-1111-1111-1111-111111111111/resourceGroups/test/providers/Microsoft.Fabric/capacities/testf2"
CAPACITY = "22222222-2222-2222-2222-222222222222"


class Credential:
    def get_token(self, *_args, **_kwargs):
        return AccessToken("test-token", 4_102_444_800)


def resource(state, sku="F2"):
    return {
        "id": RESOURCE,
        "name": "testf2",
        "sku": {"name": sku},
        "properties": {"state": state, "provisioningState": "Succeeded"},
    }


@pytest.mark.parametrize(
    "action,states,path",
    [
        ("resume", ["Paused", "Resuming", "Active"], "/resume"),
        ("pause", ["Active", "Pausing", "Paused"], "/suspend"),
        ("pause", ["Resuming", "Active", "Paused"], "/suspend"),
    ],
)
def test_state_change_submits_once_and_polls(action, states, path):
    requests = []
    marks = []

    def handler(request):
        requests.append(request)
        assert request.url.params["api-version"] == "2023-11-01"
        assert request.headers["authorization"] == "Bearer test-token"
        if request.method == "POST":
            assert request.url.path == RESOURCE + path
            if action == "resume":
                assert marks == ["resumed"]
            return httpx.Response(202)
        return httpx.Response(200, json=resource(states.pop(0)))

    with F2Capacity(
        RESOURCE,
        CAPACITY,
        Credential(),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
    ) as capacity:
        assert (
            capacity.change(action, before_resume=lambda: marks.append("resumed"))
            is True
        )
    assert sum(request.method == "POST" for request in requests) == 1
    assert not states


@pytest.mark.parametrize("action,state", [("resume", "Active"), ("pause", "Paused")])
def test_already_desired_state_has_no_mutations(action, state):
    requests = []

    def handler(request):
        requests.append(request.method)
        return httpx.Response(200, json=resource(state))

    with F2Capacity(
        RESOURCE, CAPACITY, Credential(), transport=httpx.MockTransport(handler)
    ) as capacity:
        assert capacity.change(action) is False
    assert requests == ["GET"]


def test_non_f2_and_mismatched_fabric_identity_are_rejected_before_resume():
    requests = []

    def arm(request):
        requests.append(request.method)
        return httpx.Response(200, json=resource("Paused", "F64"))

    with F2Capacity(
        RESOURCE, CAPACITY, Credential(), transport=httpx.MockTransport(arm)
    ) as capacity:
        with pytest.raises(RuntimeError, match="not F2"):
            capacity.change("resume")
        with (
            FabricApi(
                Credential(),
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(
                        200,
                        json={
                            "value": [
                                {"id": CAPACITY, "displayName": "other", "sku": "F2"}
                            ]
                        },
                    )
                ),
            ) as api,
            pytest.raises(RuntimeError, match="do not match"),
        ):
            capacity.verify_fabric_identity(api)
    assert requests == ["GET"]


def test_timeout_keeps_resume_ownership_for_failure_cleanup():
    marks = []
    requests = []

    def handler(request):
        requests.append(request.method)
        if request.method == "POST":
            return httpx.Response(202)
        return httpx.Response(
            200, json=resource("Paused" if len(requests) == 1 else "Resuming")
        )

    with (
        F2Capacity(
            RESOURCE,
            CAPACITY,
            Credential(),
            transport=httpx.MockTransport(handler),
            sleep=lambda _: None,
            attempts=2,
        ) as capacity,
        pytest.raises(TimeoutError),
    ):
        capacity.change("resume", before_resume=lambda: marks.append(True))
    assert marks == [True]
    assert requests.count("POST") == 1


def test_capacity_identity_follows_all_pages():
    calls = []

    def handler(request):
        calls.append(str(request.url))
        if request.url.params.get("page") == "2":
            return httpx.Response(
                200,
                json={
                    "value": [{"id": CAPACITY, "displayName": "testf2", "sku": "F2"}]
                },
            )
        return httpx.Response(
            200, json={"value": [], "continuationUri": "/capacities?page=2"}
        )

    with (
        F2Capacity(RESOURCE, CAPACITY, Credential()) as capacity,
        FabricApi(Credential(), transport=httpx.MockTransport(handler)) as api,
    ):
        assert capacity.verify_fabric_identity(api)["id"] == CAPACITY
    assert len(calls) == 2

# Copyright The Orca Authors
# SPDX-License-Identifier: Apache-2.0

import copy
import json
import hashlib
from pathlib import Path

from kubernetes import client as k8s
from fastapi import HTTPException

import pytest

from opensandbox_server.api.schema import ImageSpec
from opensandbox_server.config import (
    AppConfig,
    KubernetesRuntimeConfig,
    RuntimeConfig,
    SecureRuntimeConfig,
)
from opensandbox_server.services.k8s.batchsandbox_provider import BatchSandboxProvider
from opensandbox_server.services.k8s.batchsandbox_provider import _package_plan, _package_init_container
from opensandbox_server.services.k8s.security_context import (
    build_security_context_from_dict,
    serialize_security_context_to_dict,
)


PACKAGE_FIXTURES = json.loads(Path(__file__).with_name("environment-packages.json").read_text())


@pytest.mark.parametrize("case", PACKAGE_FIXTURES["valid"], ids=lambda case: case["name"])
def test_package_plan_matches_typescript_golden(case):
    normalized = _package_plan(json.dumps({"version": 1, "packages": case["packages"]}))
    assert normalized == case["normalized"]
    revision = hashlib.sha256(json.dumps(normalized, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    assert revision == case["revision"]
    assert case["revision"] in _package_init_container("image", normalized, {})["command"][-1]


@pytest.mark.parametrize("case", PACKAGE_FIXTURES["invalid"], ids=lambda case: case["name"])
def test_package_plan_rejects_typescript_golden(case):
    with pytest.raises(ValueError, match="specification"):
        _package_plan(json.dumps({"version": 1, "packages": case["packages"]}))


def test_package_spec_length_uses_utf16_units():
    assert _package_plan(json.dumps({"version": 1, "packages": {"npm": ["🚀" * 512]}}))
    with pytest.raises(ValueError, match="specification"):
        _package_plan(json.dumps({"version": 1, "packages": {"npm": ["🚀" * 513]}}))


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", [
    "ENVIRONMENT_PACKAGES_INSTALL_FAILED", "ENVIRONMENT_PACKAGES_START_FAILED",
    "ENVIRONMENT_PACKAGES_STORAGE_LIMIT", "ENVIRONMENT_PACKAGES_POD_EVICTED",
])
async def test_package_failure_during_creation_rolls_back_without_logs(
    k8s_service, mock_workload, create_sandbox_request, reason
):
    k8s_service.app_config.kubernetes.sandbox_create_timeout_seconds = 1
    k8s_service.app_config.kubernetes.sandbox_create_poll_interval_seconds = 0.01
    k8s_service.workload_provider.get_workload.return_value = mock_workload
    k8s_service.workload_provider.get_status.return_value = {
        "state": "Failed", "reason": reason, "message": "installer-private-output",
    }
    with pytest.raises(HTTPException) as exc:
        await k8s_service.create_sandbox(create_sandbox_request)
    assert exc.value.status_code == 500
    assert exc.value.detail["code"] == reason
    assert "installer-private-output" not in json.dumps(exc.value.detail)
    k8s_service.workload_provider.delete_workload.assert_called_once()


TRUSTED_IMAGE = "registry.example/orca@sha256:" + "b" * 64
TRUSTED_ENTRYPOINT = ["/opt/orca/start.sh"]
ISOLATION_EXTENSIONS = {
    "bootstrap.execd.isolation": "enable",
    "orca.fuse.device": "enable",
}
DEFAULT_PREFIXES = ["ghcr.io/orca-ae"]
OFFICIAL_WORKLOADS = [
    ("orca-opensandbox-code-interpreter", ["/opt/code-interpreter/code-interpreter.sh"]),
    ("sandbox-harness-claude-code", ["/usr/local/bin/orca-sandbox-harness"]),
    ("sandbox-harness-dsh", ["/usr/local/bin/orca-sandbox-harness"]),
]
MIRROR_PREFIXES = ["registry.example.com:5000/mirror/orca", "docker.io/orca-mirror"]


@pytest.mark.parametrize("image_pull_policy", ["IfNotPresent", "Always", "Never"])
def test_package_preparation_is_a_separate_credential_free_init_container(
    mock_k8s_client, image_pull_policy
):
    packages = {
        "apt": ["jq"],
        "cargo": ["ripgrep"],
        "gem": ["rake"],
        "go": ["example.com/tool@v1.0.0"],
        "npm": ["typescript"],
        "pip": ["requests"],
    }
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    provider.image_pull_policy = image_pull_policy
    provider.create_workload(
        sandbox_id="packages-id",
        namespace="test-ns",
        image_spec=ImageSpec(uri="ghcr.io/orca-ae/sandbox-harness-claude-code:latest"),
        entrypoint=["/usr/local/bin/orca-sandbox-harness"],
        env={"LITELLM_API_KEY": "runtime-only-sentinel", "EXECD_ACCESS_TOKEN": "a" * 64},
        resource_limits={"cpu": "1", "memory": "2Gi"},
        labels={},
        expires_at=None,
        execd_image="execd:test",
        extensions={
            **ISOLATION_EXTENSIONS,
            "orca.environment.packages.v1": json.dumps({"version": 1, "packages": packages}),
        },
    )
    pod = mock_k8s_client.create_custom_object.call_args.kwargs["body"]["spec"]["template"]["spec"]
    installer = next(
        item for item in pod["initContainers"] if item["name"] == "orca-environment-packages"
    )
    assert "runtime-only-sentinel" not in json.dumps(installer)
    assert installer["volumeMounts"] == [
        {"name": "orca-environment-packages", "mountPath": "/opt/orca-environment-packages"}
    ]
    runtime = next(item for item in pod["containers"] if item["name"] == "sandbox")
    assert installer.get("imagePullPolicy") == image_pull_policy
    assert runtime["imagePullPolicy"] == image_pull_policy
    assert {
        "name": "orca-environment-packages",
        "mountPath": "/opt/orca-environment-packages",
        "readOnly": True,
    } in runtime["volumeMounts"]
    assert pod["shareProcessNamespace"] is False
    assert pod["automountServiceAccountToken"] is False
    script = installer["command"][-1]
    assert script.index("apt-get install") < script.index("pip install")
    for command in ("cargo install", "gem install", "go install", "npm install", "pip install"):
        assert command in script


@pytest.mark.parametrize(
    "packages",
    [
        {"brew": ["jq"]},
        {"apt": ["--allow-unauthenticated"]},
        {"pip": ["requests\nother"]},
        {"gem": "rake"},
    ],
)
def test_invalid_package_plans_fail_before_workload_creation(packages):
    with pytest.raises(ValueError):
        _package_plan(json.dumps({"version": 1, "packages": packages}))


def test_package_specs_are_shell_arguments_and_go_versions_are_independent():
    container = _package_init_container(
        "image",
        {
            "npm": ["name; touch /tmp/injected"],
            "go": ["example.com/a@v1.0.0", "example.com/b@v2.0.0"],
        },
        {},
    )
    script = container["command"][-1]
    assert "-- 'name; touch /tmp/injected'" in script
    assert "go install example.com/a@v1.0.0\ngo install example.com/b@v2.0.0" in script


@pytest.mark.parametrize("phase", ["", "Pending", "Failed"])
def test_package_preparation_returns_allocated_without_hiding_failure(mock_k8s_client, phase):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    workload = {
        "metadata": {"annotations": {"sandbox.opensandbox.io/endpoints": '["10.0.0.1"]'}},
        "status": {"phase": phase},
        "spec": {"template": {"spec": {"initContainers": [{"name": "orca-environment-packages"}]}}},
    }
    assert provider.get_status(workload)["state"] == (
        "Failed" if phase == "Failed" else "Allocated"
    )
    if phase == "Pending":
        workload["spec"]["template"]["spec"]["initContainers"] = []
        assert provider.get_status(workload)["state"] == "Pending"


@pytest.mark.parametrize(("state", "reason"), [
    (k8s.V1ContainerState(terminated=k8s.V1ContainerStateTerminated(exit_code=42, message="installer-private-output")), "ENVIRONMENT_PACKAGES_INSTALL_FAILED"),
    (k8s.V1ContainerState(waiting=k8s.V1ContainerStateWaiting(reason="CrashLoopBackOff", message="installer-private-output")), "ENVIRONMENT_PACKAGES_START_FAILED"),
    (k8s.V1ContainerState(waiting=k8s.V1ContainerStateWaiting(reason="ImagePullBackOff", message="installer-private-output")), "ENVIRONMENT_PACKAGES_START_FAILED"),
])
def test_package_init_failure_is_terminal_and_redacted(mock_k8s_client, state, reason):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    container = k8s.V1ContainerStatus(name="orca-environment-packages", image="image", image_id="image", ready=False, restart_count=1, state=state)
    mock_k8s_client.list_pods.return_value = [k8s.V1Pod(status=k8s.V1PodStatus(init_container_statuses=[container]))]
    workload = {
        "metadata": {"name": "package-sandbox", "namespace": "test-ns", "annotations": {"sandbox.opensandbox.io/endpoints": '["10.0.0.1"]'}},
        "status": {"phase": "Pending"},
        "spec": {"template": {"spec": {"initContainers": [{"name": "orca-environment-packages"}]}}},
    }
    result = provider.get_status(workload)
    assert result["state"] == "Failed"
    assert result["reason"] == reason
    assert "installer-private-output" not in json.dumps(result)
    mock_k8s_client.list_pods.assert_called_once_with(namespace="test-ns", label_selector="batch-sandbox.sandbox.opensandbox.io/name=package-sandbox")


@pytest.mark.parametrize("recovery", ["running", "backoff"])
def test_package_image_pull_error_can_recover_or_reach_backoff(mock_k8s_client, recovery):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    container = k8s.V1ContainerStatus(name="orca-environment-packages", image="image", image_id="image", ready=False, restart_count=0, state=k8s.V1ContainerState(waiting=k8s.V1ContainerStateWaiting(reason="ErrImagePull", message="installer-private-output")))
    mock_k8s_client.list_pods.return_value = [k8s.V1Pod(status=k8s.V1PodStatus(init_container_statuses=[container]))]
    workload = {
        "metadata": {"name": "package-sandbox", "namespace": "test-ns", "annotations": {"sandbox.opensandbox.io/endpoints": '["10.0.0.1"]'}},
        "status": {"phase": "Pending"},
        "spec": {"template": {"spec": {"initContainers": [{"name": "orca-environment-packages"}]}}},
    }
    result = provider.get_status(workload)
    assert result["state"] == "Allocated"
    assert "installer-private-output" not in json.dumps(result)
    if recovery == "running":
        container.state = k8s.V1ContainerState(running=k8s.V1ContainerStateRunning())
        assert provider.get_status(workload)["state"] == "Allocated"
        workload["status"]["phase"] = "Running"
        assert provider.get_status(workload)["state"] == "Running"
    else:
        container.state.waiting.reason = "ImagePullBackOff"
        assert provider.get_status(workload)["reason"] == "ENVIRONMENT_PACKAGES_START_FAILED"


@pytest.mark.parametrize(("pod_message", "reason"), [
    ("Pod ephemeral-storage usage exceeds limit: installer-private-output", "ENVIRONMENT_PACKAGES_STORAGE_LIMIT"),
    ("The node was low on resource: installer-private-output", "ENVIRONMENT_PACKAGES_POD_EVICTED"),
])
def test_package_pod_eviction_is_terminal_and_redacted(mock_k8s_client, pod_message, reason):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    mock_k8s_client.list_pods.return_value = [k8s.V1Pod(status=k8s.V1PodStatus(reason="Evicted", message=pod_message))]
    workload = {
        "metadata": {"name": "package-sandbox", "namespace": "test-ns"},
        "status": {"phase": "Failed"},
        "spec": {"template": {"spec": {"initContainers": [{"name": "orca-environment-packages"}]}}},
    }
    result = provider.get_status(workload)
    assert result["state"] == "Failed"
    assert result["reason"] == reason
    assert "installer-private-output" not in json.dumps(result)


@pytest.mark.parametrize(
    "change",
    [
        "shared-volume",
        "writable-runtime",
        "no-runtime-mount",
        "credential-env",
        "unbounded-installer",
        "service-account",
        "ephemeral-writer",
        "execd-auth",
        "installer-pull-policy",
        "runtime-pull-policy",
    ],
)
def test_template_cannot_weaken_package_preparation(mock_k8s_client, monkeypatch, change):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    merge = provider.template_manager.merge_with_runtime_values

    def mutate(manifest):
        result = merge(manifest)
        pod = result["spec"]["template"]["spec"]
        installer = next(
            item for item in pod["initContainers"] if item["name"] == "orca-environment-packages"
        )
        runtime = next(item for item in pod["containers"] if item["name"] == "sandbox")
        if change == "shared-volume":
            pod["volumes"] = [
                {
                    "name": "orca-environment-packages",
                    "persistentVolumeClaim": {"claimName": "shared"},
                }
            ]
        elif change == "writable-runtime":
            next(
                mount
                for mount in runtime["volumeMounts"]
                if mount["name"] == "orca-environment-packages"
            )["readOnly"] = False
        elif change == "no-runtime-mount":
            runtime["volumeMounts"] = [
                mount
                for mount in runtime["volumeMounts"]
                if mount["name"] != "orca-environment-packages"
            ]
        elif change == "credential-env":
            installer["envFrom"] = [{"secretRef": {"name": "runtime-credentials"}}]
        elif change == "service-account":
            pod["automountServiceAccountToken"] = True
        elif change == "ephemeral-writer":
            pod["ephemeralContainers"] = [
                {"name": "debug", "volumeMounts": copy.deepcopy(installer["volumeMounts"])}
            ]
        elif change == "execd-auth":
            next(entry for entry in runtime["env"] if entry["name"] == "EXECD_ACCESS_TOKEN")["value"] = "f" * 64
        elif change == "installer-pull-policy":
            installer["imagePullPolicy"] = "Always"
        elif change == "runtime-pull-policy":
            runtime["imagePullPolicy"] = "Always"
        else:
            installer["resources"] = {}
        return result

    monkeypatch.setattr(provider.template_manager, "merge_with_runtime_values", mutate)
    with pytest.raises(ValueError, match="package"):
        provider.create_workload(
            sandbox_id="packages-id",
            namespace="test-ns",
            image_spec=ImageSpec(uri="ghcr.io/orca-ae/sandbox-harness-claude-code:v1"),
            entrypoint=["/usr/local/bin/orca-sandbox-harness"],
            env={"EXECD_ACCESS_TOKEN": "a" * 64},
            resource_limits={"cpu": "1", "memory": "2Gi"},
            labels={},
            expires_at=None,
            execd_image="execd:test",
            extensions={
                **ISOLATION_EXTENSIONS,
                "orca.environment.packages.v1": json.dumps(
                    {"version": 1, "packages": {"apt": ["jq"]}}
                ),
            },
        )
    mock_k8s_client.create_custom_object.assert_not_called()


@pytest.fixture(autouse=True)
def clear_trust_environment(monkeypatch):
    for name in (
        "ORCA_TRUSTED_SANDBOX_REPOSITORIES",
        "ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES",
        "ORCA_TRUSTED_SANDBOX_WORKLOADS",
        "ORCA_TRUSTED_NON_FUSE_WORKLOADS",
        "ORCA_ALLOW_TAGGED_SANDBOX_IMAGES",
    ):
        monkeypatch.delenv(name, raising=False)


def _submit_image(provider, image, entrypoint):
    return provider.create_workload(
        sandbox_id="test-id", namespace="test-ns",
        image_spec=ImageSpec(uri=image), entrypoint=entrypoint, env={},
        resource_limits={"cpu": "1", "memory": "1Gi"}, labels={},
        expires_at=None, execd_image="execd:latest", extensions=ISOLATION_EXTENSIONS,
    )


@pytest.mark.parametrize("prefix", DEFAULT_PREFIXES)
@pytest.mark.parametrize(("name", "entrypoint"), OFFICIAL_WORKLOADS)
@pytest.mark.parametrize("suffix", [
    ":0.4.4-rc.6", ":future-release", ":latest",
    "@sha256:" + "a" * 64, ":future-release@sha256:" + "c" * 64,
])
def test_official_repositories_accept_changing_versions(
    mock_k8s_client, prefix, name, entrypoint, suffix,
):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    _submit_image(provider, f"{prefix}/{name}{suffix}", entrypoint)
    mock_k8s_client.create_custom_object.assert_called_once()
    pod = mock_k8s_client.create_custom_object.call_args.kwargs["body"]["spec"]["template"]["spec"]
    assert pod["runtimeClassName"] == "gvisor"
    assert all("hostPath" not in volume for volume in pod.get("volumes", []))


@pytest.mark.parametrize(("name", "entrypoint"), OFFICIAL_WORKLOADS)
@pytest.mark.parametrize("reference", [
    "evil.example/orca-ae/{name}:v1",
    "ghcr.io.evil.example/orca-ae/{name}:v1",
    "docker.io/orca-ae/{name}:v1",
    "ghcr.io/attacker/{name}:v1",
    "ghcr.io/orca-ae-evil/{name}:v1",
    "ghcr.io/orca-ae/{name}-evil:v1",
    "ghcr.io/orca-ae/{name}/child:v1",
    "ghcr.io/orca-ae/nested/{name}:v1",
    "ghcr.io/orca-ae/unrelated:v1",
    "orca-ae/{name}:v1",
    "https://ghcr.io/orca-ae/{name}:v1",
    "ghcr.io:443/orca-ae/{name}:v1",
    "ghcr.io/orca-ae/{name}",
    "ghcr.io/orca-ae/{name}:",
    "ghcr.io/orca-ae/{name}:bad tag",
    "ghcr.io/orca-ae/{name}:v1\n",
    "ghcr.io/orca-ae/{name}:v1/path",
    "ghcr.io/orca-ae/{name}@sha256:abc",
    "ghcr.io/orca-ae/{name}:v1@sha256:abc",
    "ghcr.io/orca-ae/{name}@sha512:" + "a" * 128,
    "ghcr.io/orca-ae/{name}@sha256:" + "A" * 64,
    "ghcr.io/orca-ae/{name}@sha256:" + "a" * 65,
    "ghcr.io/orca-ae/{name}:" + "a" * 129,
    "ghcr.io//orca-ae/{name}:v1",
    "ghcr.io/orca-ae/../{name}:v1",
    "GHCR.IO/orca-ae/{name}:v1",
    "ghcr.io/orca-ae/{name}:v1?query",
    "ghcr.io/orca-ae/{name}:v1#fragment",
    "ghcr.io/orca-ae/{name}:v1@sha256:" + "a" * 64 + "@sha256:" + "b" * 64,
])
def test_repository_policy_rejects_untrusted_or_malformed_references(
    mock_k8s_client, name, entrypoint, reference,
):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    with pytest.raises(ValueError):
        _submit_image(provider, reference.format(name=name), entrypoint)
    mock_k8s_client.create_custom_object.assert_not_called()


@pytest.mark.parametrize(("name", "entrypoint"), OFFICIAL_WORKLOADS)
@pytest.mark.parametrize("wrong_entrypoint", [
    ["/bin/sh"], ["/opt/code-interpreter/code-interpreter.sh", "--custom"],
    ["/usr/local/bin/orca-sandbox-harness", "--custom"],
])
def test_repository_policy_rejects_custom_entrypoints(
    mock_k8s_client, name, entrypoint, wrong_entrypoint,
):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    with pytest.raises(ValueError, match="not authorized"):
        _submit_image(provider, f"ghcr.io/orca-ae/{name}:v1", wrong_entrypoint)
    mock_k8s_client.create_custom_object.assert_not_called()


@pytest.mark.parametrize("index", [0, 1])
def test_official_entrypoints_cannot_be_swapped(mock_k8s_client, index):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    with pytest.raises(ValueError, match="not authorized"):
        _submit_image(provider, f"ghcr.io/orca-ae/{OFFICIAL_WORKLOADS[index][0]}:v1",
                      OFFICIAL_WORKLOADS[1 - index][1])
    mock_k8s_client.create_custom_object.assert_not_called()


@pytest.mark.parametrize("policy", [[], [{
    "repository": "private.example/custom/sandbox", "entrypoint": ["/start"],
}]])
def test_repository_override_replaces_defaults(mock_k8s_client, monkeypatch, policy):
    monkeypatch.setenv("ORCA_TRUSTED_SANDBOX_REPOSITORIES", json.dumps(policy))
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    with pytest.raises(ValueError, match="not authorized"):
        _submit_image(provider, "ghcr.io/orca-ae/orca-opensandbox-code-interpreter:v1",
                      OFFICIAL_WORKLOADS[0][1])
    mock_k8s_client.create_custom_object.assert_not_called()
    if policy:
        _submit_image(provider, "private.example/custom/sandbox:v2", ["/start"])
        mock_k8s_client.create_custom_object.assert_called_once()


@pytest.mark.parametrize("raw", [
    "", "not json", "null", "{}", "[1]",
    '[{"repository":"ghcr.io/orca-ae/*","entrypoint":["/start"]}]',
    '[{"repository":"ghcr.io/orca-ae/image:v1","entrypoint":["/start"]}]',
    '[{"repository":"ghcr.io/orca-ae/image","entrypoint":[]}]',
    '[{"repository":"ghcr.io/orca-ae/image","entrypoint":[1]}]',
    '[{"repository":"ghcr.io/orca-ae/image","entrypoint":["/start"],"typo":true}]',
])
def test_invalid_repository_configuration_fails_closed(mock_k8s_client, monkeypatch, raw):
    monkeypatch.setenv("ORCA_TRUSTED_SANDBOX_REPOSITORIES", raw)
    with pytest.raises(RuntimeError, match="ORCA_TRUSTED_SANDBOX_REPOSITORIES"):
        BatchSandboxProvider(mock_k8s_client, _app_config())


# A prefix names where the official images live; it is expanded to exact
# <prefix>/<name> repositories and never matched as a string prefix.
@pytest.mark.parametrize("prefixes", [MIRROR_PREFIXES, DEFAULT_PREFIXES + MIRROR_PREFIXES])
@pytest.mark.parametrize(("name", "entrypoint"), OFFICIAL_WORKLOADS)
def test_repository_prefixes_trust_official_images_under_each_prefix(
    mock_k8s_client, monkeypatch, prefixes, name, entrypoint,
):
    monkeypatch.setenv("ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES", json.dumps(prefixes))
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    for prefix in prefixes:
        for suffix in (":v1", "@sha256:" + "a" * 64):
            _submit_image(provider, f"{prefix}/{name}{suffix}", entrypoint)
    assert mock_k8s_client.create_custom_object.call_count == 2 * len(prefixes)


@pytest.mark.parametrize("prefixes", [[], MIRROR_PREFIXES])
@pytest.mark.parametrize(("name", "entrypoint"), OFFICIAL_WORKLOADS)
def test_repository_prefixes_replace_the_default_prefix(
    mock_k8s_client, monkeypatch, prefixes, name, entrypoint,
):
    monkeypatch.setenv("ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES", json.dumps(prefixes))
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    with pytest.raises(ValueError, match="not authorized"):
        _submit_image(provider, f"ghcr.io/orca-ae/{name}:v1", entrypoint)
    mock_k8s_client.create_custom_object.assert_not_called()


@pytest.mark.parametrize(("name", "entrypoint"), OFFICIAL_WORKLOADS)
@pytest.mark.parametrize("reference", [
    "registry.example.com/mirror/orca/{name}:v1",
    "registry.example.com:5000/mirror/{name}:v1",
    "registry.example.com:5000/mirror/orca/nested/{name}:v1",
    "registry.example.com:5000/mirror/orca-evil/{name}:v1",
    "registry.example.com:5000/mirror/orca/{name}-evil:v1",
    "orca-mirror/{name}:v1",
    "index.docker.io/orca-mirror/{name}:v1",
    "docker.io/orca-mirror/{name}",
    "docker.io/orca-mirror/unrelated:v1",
])
def test_repository_prefixes_match_exact_repositories_only(
    mock_k8s_client, monkeypatch, name, entrypoint, reference,
):
    monkeypatch.setenv("ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES", json.dumps(MIRROR_PREFIXES))
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    with pytest.raises(ValueError):
        _submit_image(provider, reference.format(name=name), entrypoint)
    mock_k8s_client.create_custom_object.assert_not_called()


@pytest.mark.parametrize("index", [0, 1])
def test_repository_prefixes_keep_exact_entrypoints(mock_k8s_client, monkeypatch, index):
    monkeypatch.setenv("ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES", json.dumps(MIRROR_PREFIXES))
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    image = f"{MIRROR_PREFIXES[0]}/{OFFICIAL_WORKLOADS[index][0]}:v1"
    for command in (OFFICIAL_WORKLOADS[1 - index][1],
                    OFFICIAL_WORKLOADS[index][1] + ["--custom"], ["/bin/sh"]):
        with pytest.raises(ValueError, match="not authorized"):
            _submit_image(provider, image, command)
    mock_k8s_client.create_custom_object.assert_not_called()


@pytest.mark.parametrize("raw", [
    "", "not json", "null", "{}", '"ghcr.io/orca-ae"', "[1]", '[""]',
    '["ghcr.io/orca-ae/"]', '["ghcr.io/orca-ae/*"]', '["https://ghcr.io/orca-ae"]',
    '["GHCR.IO/orca-ae"]', '["ghcr.io/orca-ae:v1"]',
    '["ghcr.io/orca-ae@sha256:' + "a" * 64 + '"]',
    '["ghcr.io//orca-ae"]', '["ghcr.io/orca-ae",1]',
])
def test_invalid_repository_prefixes_fail_closed(mock_k8s_client, monkeypatch, raw):
    monkeypatch.setenv("ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES", raw)
    with pytest.raises(RuntimeError, match="ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES"):
        BatchSandboxProvider(mock_k8s_client, _app_config())


# An explicit repository policy replaces the official repositories, so a prefix
# list beside it would be silently ignored; any value of both fails closed.
@pytest.mark.parametrize("repositories", ["[]", json.dumps([
    {"repository": "private.example/custom/sandbox", "entrypoint": ["/start"]},
])])
@pytest.mark.parametrize("prefixes", ['["ghcr.io/orca-ae"]', "[]", ""])
def test_repository_prefixes_cannot_combine_with_explicit_repositories(
    mock_k8s_client, monkeypatch, repositories, prefixes,
):
    monkeypatch.setenv("ORCA_TRUSTED_SANDBOX_REPOSITORIES", repositories)
    monkeypatch.setenv("ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES", prefixes)
    with pytest.raises(RuntimeError, match=(
        "at most one of ORCA_TRUSTED_SANDBOX_REPOSITORIES and "
        "ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES"
    )):
        BatchSandboxProvider(mock_k8s_client, _app_config())


@pytest.mark.parametrize("tagged", [False, True])
@pytest.mark.parametrize("exact_only", [False, True])
def test_legacy_exact_workloads_remain_exact(mock_k8s_client, monkeypatch, tagged, exact_only):
    image = "private.example/custom/sandbox" + (":v1" if tagged else "@sha256:" + "a" * 64)
    if exact_only:
        monkeypatch.setenv("ORCA_TRUSTED_SANDBOX_REPOSITORIES", "[]")
    monkeypatch.setenv("ORCA_TRUSTED_SANDBOX_WORKLOADS", json.dumps([
        {"image": image, "entrypoint": ["/start"]},
    ]))
    if tagged:
        monkeypatch.setenv("ORCA_ALLOW_TAGGED_SANDBOX_IMAGES", "true")
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    _submit_image(provider, image, ["/start"])
    mock_k8s_client.create_custom_object.assert_called_once()
    mock_k8s_client.create_custom_object.reset_mock()
    for uri, command in [(image, ["/start", "extra"]),
                         ("private.example/custom/sandbox:v2", ["/start"]),
                         ("private.example/custom/sandbox@sha256:" + "b" * 64, ["/start"])]:
        with pytest.raises(ValueError, match="not authorized"):
            _submit_image(provider, uri, command)
    mock_k8s_client.create_custom_object.assert_not_called()
    if not exact_only:
        _submit_image(provider, "ghcr.io/orca-ae/orca-opensandbox-code-interpreter:v2",
                      OFFICIAL_WORKLOADS[0][1])
        mock_k8s_client.create_custom_object.assert_called_once()


def _app_config(
    *,
    runtime_type: str = "gvisor",
    runtime_class: str = "gvisor",
) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(type="kubernetes", execd_image="execd:test"),
        kubernetes=KubernetesRuntimeConfig(namespace="test-ns"),
        secure_runtime=SecureRuntimeConfig(
            type=runtime_type,
            k8s_runtime_class=runtime_class,
        ),
    )


def _trust_workload(monkeypatch) -> None:
    monkeypatch.setenv(
        "ORCA_TRUSTED_SANDBOX_WORKLOADS",
        json.dumps(
            [{"image": TRUSTED_IMAGE, "entrypoint": TRUSTED_ENTRYPOINT}]
        ),
    )


def _create_isolated_workload(
    mock_k8s_client,
    monkeypatch,
    *,
    provider: BatchSandboxProvider | None = None,
) -> tuple[BatchSandboxProvider, dict]:
    _trust_workload(monkeypatch)
    provider = provider or BatchSandboxProvider(mock_k8s_client, _app_config())
    mock_k8s_client.create_custom_object.return_value = {
        "metadata": {"name": "test-id", "uid": "test-uid"}
    }
    provider.create_workload(
        sandbox_id="test-id",
        namespace="test-ns",
        image_spec=ImageSpec(uri=TRUSTED_IMAGE),
        entrypoint=TRUSTED_ENTRYPOINT,
        env={},
        resource_limits={"cpu": "1", "memory": "1Gi"},
        labels={},
        expires_at=None,
        execd_image="execd:latest",
        extensions=ISOLATION_EXTENSIONS,
    )
    body = mock_k8s_client.create_custom_object.call_args.kwargs["body"]
    return provider, body


@pytest.mark.parametrize("extensions", [
    {"bootstrap.execd.isolation": "enable"},
    {"bootstrap.execd.isolation": "enable", "orca.fuse.device": "disable"},
])
def test_explicit_non_fuse_workload_preserves_isolation(
    mock_k8s_client, monkeypatch, extensions,
):
    monkeypatch.setenv("ORCA_TRUSTED_NON_FUSE_WORKLOADS", json.dumps([
        {"image": TRUSTED_IMAGE, "entrypoint": TRUSTED_ENTRYPOINT},
    ]))
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    provider.create_workload(
        sandbox_id="test-id", namespace="test-ns",
        image_spec=ImageSpec(uri=TRUSTED_IMAGE), entrypoint=TRUSTED_ENTRYPOINT, env={},
        resource_limits={"cpu": "1", "memory": "1Gi"}, labels={},
        expires_at=None, execd_image="execd:latest", extensions=extensions,
    )
    pod = mock_k8s_client.create_custom_object.call_args.kwargs["body"]["spec"]["template"]["spec"]
    assert pod["runtimeClassName"] == "gvisor"
    assert pod["automountServiceAccountToken"] is False
    assert all("hostPath" not in volume for volume in pod.get("volumes", []))
    sandbox = next(c for c in pod["containers"] if c["name"] == "sandbox")
    assert set(sandbox["securityContext"]["capabilities"]["add"]) == {"SETFCAP", "SYS_ADMIN"}
    assert not sandbox["securityContext"].get("privileged", False)
    assert all(m["mountPath"] != "/dev/fuse" for m in sandbox.get("volumeMounts", []))


@pytest.mark.parametrize(("image", "entrypoint", "extensions"), [
    (TRUSTED_IMAGE, ["/bin/sh"], {"bootstrap.execd.isolation": "enable"}),
    ("registry.example/other@sha256:" + "b" * 64, TRUSTED_ENTRYPOINT,
     {"bootstrap.execd.isolation": "enable"}),
    (TRUSTED_IMAGE, TRUSTED_ENTRYPOINT, {}),
    (TRUSTED_IMAGE, TRUSTED_ENTRYPOINT, ISOLATION_EXTENSIONS),
])
def test_non_fuse_trust_cannot_change_entrypoint_image_isolation_or_enable_fuse(
    mock_k8s_client, monkeypatch, image, entrypoint, extensions,
):
    monkeypatch.setenv("ORCA_TRUSTED_NON_FUSE_WORKLOADS", json.dumps([
        {"image": TRUSTED_IMAGE, "entrypoint": TRUSTED_ENTRYPOINT},
    ]))
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    with pytest.raises(ValueError):
        provider.create_workload(
            sandbox_id="test-id", namespace="test-ns",
            image_spec=ImageSpec(uri=image), entrypoint=entrypoint, env={},
            resource_limits={"cpu": "1", "memory": "1Gi"}, labels={},
            expires_at=None, execd_image="execd:latest", extensions=extensions,
        )
    mock_k8s_client.create_custom_object.assert_not_called()


@pytest.mark.parametrize("policy", ["{}", "not-json", '[{"image":"image:tag","entrypoint":["/start"]}]'])
def test_non_fuse_trust_policy_fails_closed(mock_k8s_client, monkeypatch, policy):
    monkeypatch.setenv("ORCA_TRUSTED_NON_FUSE_WORKLOADS", policy)
    with pytest.raises(RuntimeError, match="ORCA_TRUSTED_NON_FUSE_WORKLOADS"):
        BatchSandboxProvider(mock_k8s_client, _app_config())


def test_provider_requires_app_config(mock_k8s_client):
    with pytest.raises(ValueError, match="secure_runtime.type='gvisor'"):
        BatchSandboxProvider(mock_k8s_client)


@pytest.mark.parametrize(
    ("config", "message"),
    [
        (
            AppConfig(
                runtime=RuntimeConfig(type="kubernetes", execd_image="execd:test"),
                kubernetes=KubernetesRuntimeConfig(namespace="test-ns"),
            ),
            "secure_runtime.type='gvisor'",
        ),
        (_app_config(runtime_type="kata"), "secure_runtime.type='gvisor'"),
        (
            _app_config(runtime_class="sandboxed"),
            "secure_runtime.k8s_runtime_class='gvisor'",
        ),
    ],
)
def test_provider_rejects_non_gvisor_runtime_config(
    mock_k8s_client,
    config,
    message,
):
    with pytest.raises(ValueError, match=message):
        BatchSandboxProvider(mock_k8s_client, config)


def test_gvisor_fuse_manifest_has_no_host_runtime_escape_hatches(
    mock_k8s_client,
    monkeypatch,
):
    _, body = _create_isolated_workload(mock_k8s_client, monkeypatch)

    pod_spec = body["spec"]["template"]["spec"]
    sandbox = next(
        container
        for container in pod_spec["containers"]
        if container["name"] == "sandbox"
    )
    security_context = sandbox["securityContext"]

    assert pod_spec["runtimeClassName"] == "gvisor"
    assert "hostUsers" not in pod_spec
    assert pod_spec.get("hostNetwork", False) is False
    assert pod_spec.get("hostPID", False) is False
    assert pod_spec.get("hostIPC", False) is False
    assert all("hostPath" not in volume for volume in pod_spec.get("volumes", []))
    assert all(
        mount.get("mountPath") != "/dev/fuse"
        for mount in sandbox.get("volumeMounts", [])
    )

    assert set(security_context["capabilities"]["add"]) == {"SETFCAP", "SYS_ADMIN"}
    assert "privileged" not in security_context
    assert "procMount" not in security_context
    assert security_context["seccompProfile"] == {"type": "Unconfined"}
    assert security_context["appArmorProfile"] == {"type": "Unconfined"}


def test_isolation_requires_complete_gvisor_fuse_extensions_before_cr_creation(
    mock_k8s_client,
):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())

    with pytest.raises(ValueError, match="require bootstrap.execd.isolation=enable"):
        provider.create_workload(
            sandbox_id="test-id",
            namespace="test-ns",
            image_spec=ImageSpec(uri=TRUSTED_IMAGE),
            entrypoint=TRUSTED_ENTRYPOINT,
            env={},
            resource_limits={"cpu": "1", "memory": "1Gi"},
            labels={},
            expires_at=None,
            execd_image="execd:latest",
            extensions={"bootstrap.execd.isolation": "enable"},
        )

    mock_k8s_client.create_custom_object.assert_not_called()


def test_fuse_extension_requires_isolation_before_cr_creation(mock_k8s_client):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())

    with pytest.raises(ValueError, match="require bootstrap.execd.isolation=enable"):
        provider.create_workload(
            sandbox_id="test-id",
            namespace="test-ns",
            image_spec=ImageSpec(uri=TRUSTED_IMAGE),
            entrypoint=TRUSTED_ENTRYPOINT,
            env={},
            resource_limits={"cpu": "1", "memory": "1Gi"},
            labels={},
            expires_at=None,
            execd_image="execd:latest",
            extensions={"orca.fuse.device": "enable"},
        )

    mock_k8s_client.create_custom_object.assert_not_called()


def test_request_without_gvisor_fuse_extensions_is_rejected(mock_k8s_client):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())

    with pytest.raises(ValueError, match="require bootstrap.execd.isolation=enable"):
        provider.create_workload(
            sandbox_id="test-id",
            namespace="test-ns",
            image_spec=ImageSpec(uri=TRUSTED_IMAGE),
            entrypoint=TRUSTED_ENTRYPOINT,
            env={},
            resource_limits={"cpu": "1", "memory": "1Gi"},
            labels={},
            expires_at=None,
            execd_image="execd:latest",
        )

    mock_k8s_client.create_custom_object.assert_not_called()


def test_isolation_rejects_workload_missing_operator_allowlist(
    mock_k8s_client,
    monkeypatch,
):
    monkeypatch.delenv("ORCA_TRUSTED_SANDBOX_WORKLOADS", raising=False)
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())

    with pytest.raises(ValueError, match="ORCA_TRUSTED_SANDBOX_WORKLOADS"):
        provider.create_workload(
            sandbox_id="test-id",
            namespace="test-ns",
            image_spec=ImageSpec(uri=TRUSTED_IMAGE),
            entrypoint=TRUSTED_ENTRYPOINT,
            env={},
            resource_limits={"cpu": "1", "memory": "1Gi"},
            labels={},
            expires_at=None,
            execd_image="execd:latest",
            extensions=ISOLATION_EXTENSIONS,
        )


def test_isolation_rejects_entrypoint_mismatch(mock_k8s_client, monkeypatch):
    _trust_workload(monkeypatch)
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())

    with pytest.raises(ValueError, match="ORCA_TRUSTED_SANDBOX_WORKLOADS"):
        provider.create_workload(
            sandbox_id="test-id",
            namespace="test-ns",
            image_spec=ImageSpec(uri=TRUSTED_IMAGE),
            entrypoint=["/attacker/start.sh"],
            env={},
            resource_limits={"cpu": "1", "memory": "1Gi"},
            labels={},
            expires_at=None,
            execd_image="execd:latest",
            extensions=ISOLATION_EXTENSIONS,
        )


def test_tagged_trusted_workload_is_rejected_by_default(mock_k8s_client, monkeypatch):
    monkeypatch.delenv("ORCA_ALLOW_TAGGED_SANDBOX_IMAGES", raising=False)
    monkeypatch.setenv(
        "ORCA_TRUSTED_SANDBOX_WORKLOADS",
        json.dumps(
            [{"image": "registry.example/orca:latest", "entrypoint": ["tail"]}]
        ),
    )

    with pytest.raises(RuntimeError, match="immutable sha256 digests"):
        BatchSandboxProvider(mock_k8s_client, _app_config())


def test_pool_mode_is_not_a_supported_runtime_profile(mock_k8s_client):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())

    with pytest.raises(ValueError, match="does not support pool mode"):
        provider.create_workload(
            sandbox_id="test-id",
            namespace="test-ns",
            image_spec=ImageSpec(uri=TRUSTED_IMAGE),
            entrypoint=TRUSTED_ENTRYPOINT,
            env={},
            resource_limits={"cpu": "1", "memory": "1Gi"},
            labels={},
            expires_at=None,
            execd_image="execd:latest",
            extensions={"poolRef": "legacy-pool"},
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"network_policy": object()},
        {"credential_proxy_enabled": True},
    ],
)
def test_request_level_egress_is_not_supported(mock_k8s_client, kwargs):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())

    with pytest.raises(ValueError, match="cluster-level egress policy"):
        provider.create_workload(
            sandbox_id="test-id",
            namespace="test-ns",
            image_spec=ImageSpec(uri=TRUSTED_IMAGE),
            entrypoint=TRUSTED_ENTRYPOINT,
            env={},
            resource_limits={"cpu": "1", "memory": "1Gi"},
            labels={},
            expires_at=None,
            execd_image="execd:latest",
            extensions=ISOLATION_EXTENSIONS,
            **kwargs,
        )

    mock_k8s_client.create_custom_object.assert_not_called()


def test_legacy_host_fuse_template_is_rejected_before_cr_creation(
    mock_k8s_client,
    monkeypatch,
):
    _trust_workload(monkeypatch)
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())
    monkeypatch.setattr(
        provider.template_manager,
        "get_base_template",
        lambda: {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "sandbox",
                                "volumeMounts": [
                                    {"name": "dev-fuse", "mountPath": "/dev/fuse"}
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "dev-fuse",
                                "hostPath": {
                                    "path": "/dev/fuse",
                                    "type": "CharDevice",
                                },
                            }
                        ],
                    }
                }
            }
        },
    )

    with pytest.raises(ValueError, match="hostPath volumes"):
        provider.create_workload(
            sandbox_id="test-id",
            namespace="test-ns",
            image_spec=ImageSpec(uri=TRUSTED_IMAGE),
            entrypoint=TRUSTED_ENTRYPOINT,
            env={},
            resource_limits={"cpu": "1", "memory": "1Gi"},
            labels={},
            expires_at=None,
            execd_image="execd:latest",
            extensions=ISOLATION_EXTENSIONS,
        )

    mock_k8s_client.create_custom_object.assert_not_called()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda pod: pod["containers"][0]["securityContext"].update(
                {"privileged": True}
            ),
            "must not be privileged",
        ),
        (lambda pod: pod.update({"hostUsers": True}), "must not set hostUsers"),
        (lambda pod: pod.update({"hostNetwork": True}), "hostNetwork=true"),
        (
            lambda pod: pod["containers"][0]["securityContext"].update(
                {"procMount": "Unmasked"}
            ),
            "must not set procMount",
        ),
        (
            lambda pod: pod["containers"][0]["securityContext"]["capabilities"].update(
                {"add": ["NET_ADMIN", "SETFCAP", "SYS_ADMIN"]}
            ),
            "may add only SETFCAP and SYS_ADMIN",
        ),
    ],
)
def test_final_manifest_guard_rejects_forbidden_security_fields(
    mock_k8s_client,
    monkeypatch,
    mutation,
    message,
):
    provider, body = _create_isolated_workload(mock_k8s_client, monkeypatch)
    pod_spec = copy.deepcopy(body["spec"]["template"]["spec"])
    mutation(pod_spec)

    with pytest.raises(ValueError, match=message):
        provider._validate_gvisor_fuse_pod_spec(
            pod_spec,
            isolation_enabled=True,
        )


@pytest.mark.parametrize("operation", ["pause_sandbox", "resume_sandbox"])
def test_pause_and_resume_reject_before_any_cr_access(
    mock_k8s_client,
    operation,
):
    provider = BatchSandboxProvider(mock_k8s_client, _app_config())

    with pytest.raises(ValueError, match="gVisor in-sandbox FUSE"):
        getattr(provider, operation)("test-id", "test-ns")

    mock_k8s_client.get_custom_object.assert_not_called()
    mock_k8s_client.patch_custom_object.assert_not_called()


@pytest.mark.parametrize(
    "security_context_dict",
    [
        {
            "seccompProfile": {
                "type": "Localhost",
                "localhostProfile": "profiles/seccomp.json",
            }
        },
        {
            "appArmorProfile": {
                "type": "Localhost",
                "localhostProfile": "profiles/apparmor",
            }
        },
    ],
)
def test_security_context_round_trip_preserves_required_profiles(
    security_context_dict,
):
    security_context = build_security_context_from_dict(security_context_dict)

    assert serialize_security_context_to_dict(security_context) == security_context_dict

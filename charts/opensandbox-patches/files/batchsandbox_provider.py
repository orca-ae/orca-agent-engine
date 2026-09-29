# Copyright 2025 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
BatchSandbox-based workload provider implementation.
"""

import logging
import json
import os
import re
from datetime import datetime
from typing import Dict, List, Any, Optional

from opensandbox_server.config import (
    AppConfig,
    EGRESS_MODE_DNS,
    INGRESS_MODE_GATEWAY,
)
from opensandbox_server.extensions.keys import BOOTSTRAP_EXECD_ISOLATION_KEY
from opensandbox_server.services.helpers import format_ingress_endpoint
from opensandbox_server.api.schema import Endpoint, ImageSpec, NetworkPolicy, PlatformSpec, Volume
from opensandbox_server.services.k8s.image_pull_secret_helper import (
    build_image_pull_secret,
    build_image_pull_secret_name,
)
from opensandbox_server.services.k8s.batchsandbox_template import BatchSandboxTemplateManager
from opensandbox_server.services.k8s.client import K8sClient
from opensandbox_server.services.k8s.provider_common import (
    _build_execd_init_container,
    _build_main_container,
    _container_to_dict,
    _extract_platform_unschedulable_message_from_pod,
    _workload_platform_constraint_scope,
)
from opensandbox_server.services.k8s.volume_helper import apply_volumes_to_pod_spec
from opensandbox_server.services.k8s.workload_provider import WorkloadProvider
from opensandbox_server.services.runtime_resolver import SecureRuntimeResolver

logger = logging.getLogger(__name__)

_FUSE_DEVICE_EXTENSION = "orca.fuse.device"
_TRUSTED_WORKLOADS_ENV = "ORCA_TRUSTED_SANDBOX_WORKLOADS"
_TRUSTED_NON_FUSE_WORKLOADS_ENV = "ORCA_TRUSTED_NON_FUSE_WORKLOADS"
_TRUSTED_REPOSITORIES_ENV = "ORCA_TRUSTED_SANDBOX_REPOSITORIES"
_TRUSTED_REPOSITORY_PREFIXES_ENV = "ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES"
_ALLOW_TAGGED_IMAGES_ENV = "ORCA_ALLOW_TAGGED_SANDBOX_IMAGES"
_GVISOR_RUNTIME_TYPE = "gvisor"
_GVISOR_RUNTIME_CLASS = "gvisor"
# The official Orca sandbox images and the only entrypoint each may run with.
_OFFICIAL_IMAGES = (
    ("orca-opensandbox-code-interpreter", ("/opt/code-interpreter/code-interpreter.sh",)),
    ("sandbox-harness-claude-code", ("/usr/local/bin/orca-sandbox-harness",)),
)
# Registry/namespace prefixes the official images are trusted under: the release
# registry by default. Operators who mirror the images list their mirrors in
# ORCA_TRUSTED_SANDBOX_REPOSITORY_PREFIXES instead. Each prefix expands to the
# exact <prefix>/<name> repositories above, never to a string-prefix match.
_DEFAULT_REPOSITORY_PREFIXES = ("ghcr.io/orca-ae",)
# Require an explicit registry and an explicit tag and/or sha256 digest. Do not
# normalize aliases, infer Docker Hub, or use prefix/glob repository matching.
_REPOSITORY_PATTERN = (
    r"[a-z0-9]+(?:[.-][a-z0-9]+)*(?::[0-9]+)?/"
    r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
    r"(?:/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*)*"
)
_IMAGE_REFERENCE = re.compile(
    rf"(?P<repository>{_REPOSITORY_PATTERN})"
    r"(?::(?P<tag>[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}))?"
    r"(?:@(?P<digest>sha256:[0-9a-f]{64}))?"
)


def _image_repository(image: str) -> Optional[str]:
    match = _IMAGE_REFERENCE.fullmatch(image)
    if match and (match["tag"] or match["digest"]):
        return match["repository"]
    return None


def _load_official_repositories() -> set[tuple[str, tuple[str, ...]]]:
    """Trust each official image at exactly <prefix>/<name> with its entrypoint."""
    raw = os.environ.get(_TRUSTED_REPOSITORY_PREFIXES_ENV)
    if raw is None:
        prefixes = list(_DEFAULT_REPOSITORY_PREFIXES)
    else:
        try:
            prefixes = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"{_TRUSTED_REPOSITORY_PREFIXES_ENV} must be valid JSON"
            ) from exc
        # A prefix is valid only when every repository it yields is an exact
        # repository: no scheme, tag, digest, wildcard or trailing slash.
        if not isinstance(prefixes, list) or not all(
            isinstance(prefix, str)
            and all(
                re.fullmatch(_REPOSITORY_PATTERN, f"{prefix}/{name}")
                for name, _ in _OFFICIAL_IMAGES
            )
            for prefix in prefixes
        ):
            raise RuntimeError(
                f"{_TRUSTED_REPOSITORY_PREFIXES_ENV} must be a JSON array of exact "
                "registry/namespace prefixes such as \"ghcr.io/orca-ae\""
            )
    return {
        (f"{prefix}/{name}", entrypoint)
        for prefix in prefixes
        for name, entrypoint in _OFFICIAL_IMAGES
    }


def _load_trusted_repositories() -> set[tuple[str, tuple[str, ...]]]:
    raw = os.environ.get(_TRUSTED_REPOSITORIES_ENV)
    if raw is None:
        return _load_official_repositories()
    # An explicit repository array replaces the official repositories, so a
    # prefix list set alongside it would be silently ignored. Refuse instead.
    if _TRUSTED_REPOSITORY_PREFIXES_ENV in os.environ:
        raise RuntimeError(
            f"set at most one of {_TRUSTED_REPOSITORIES_ENV} and "
            f"{_TRUSTED_REPOSITORY_PREFIXES_ENV}; an explicit "
            f"{_TRUSTED_REPOSITORIES_ENV} replaces the official repositories"
        )
    try:
        items = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{_TRUSTED_REPOSITORIES_ENV} must be valid JSON") from exc
    if not isinstance(items, list):
        raise RuntimeError(f"{_TRUSTED_REPOSITORIES_ENV} must be a JSON array")
    trusted = set()
    for item in items:
        if not isinstance(item, dict):
            raise RuntimeError(f"{_TRUSTED_REPOSITORIES_ENV} entries must be objects")
        repository = item.get("repository")
        entrypoint = item.get("entrypoint")
        if (
            not isinstance(repository, str)
            or not re.fullmatch(_REPOSITORY_PATTERN, repository)
            or not isinstance(entrypoint, list)
            or not entrypoint
            or not all(isinstance(part, str) and part for part in entrypoint)
            or set(item) != {"repository", "entrypoint"}
        ):
            raise RuntimeError(
                f"{_TRUSTED_REPOSITORIES_ENV} entries require an exact repository "
                "without tag/digest and a non-empty entrypoint"
            )
        trusted.add((repository, tuple(entrypoint)))
    return trusted


def _load_trusted_workloads(env_name: str = _TRUSTED_WORKLOADS_ENV) -> set[tuple[str, tuple[str, ...]]]:
    raw = os.environ.get(env_name, "")
    if not raw:
        return set()
    try:
        items = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{env_name} must be valid JSON") from exc
    if not isinstance(items, list):
        raise RuntimeError(f"{env_name} must be a JSON array")

    allow_tagged_images = os.environ.get(_ALLOW_TAGGED_IMAGES_ENV) == "true"
    trusted: set[tuple[str, tuple[str, ...]]] = set()
    for item in items:
        if not isinstance(item, dict):
            raise RuntimeError(f"{env_name} entries must be objects")
        image = item.get("image")
        entrypoint = item.get("entrypoint")
        if (
            not isinstance(image, str)
            or not image
            or not isinstance(entrypoint, list)
            or not entrypoint
            or not all(isinstance(part, str) and part for part in entrypoint)
        ):
            raise RuntimeError(
                f"{env_name} entries require image and non-empty entrypoint"
            )
        if not allow_tagged_images and not re.fullmatch(r".+@sha256:[0-9a-f]{64}", image):
            raise RuntimeError(
                f"{env_name} images must use immutable sha256 digests; "
                f"set {_ALLOW_TAGGED_IMAGES_ENV}=true only for local/CI images"
            )
        trusted.add((image, tuple(entrypoint)))
    return trusted


class BatchSandboxProvider(WorkloadProvider):
    """Workload provider for BatchSandbox CRDs."""
    
    def __init__(
        self,
        k8s_client: K8sClient,
        app_config: Optional[AppConfig] = None,
    ):
        if app_config is None:
            raise ValueError(
                "BatchSandboxProvider requires Kubernetes AppConfig with "
                "secure_runtime.type='gvisor' and "
                "secure_runtime.k8s_runtime_class='gvisor'"
            )
        if app_config.runtime.type != "kubernetes":
            raise ValueError("BatchSandboxProvider requires runtime.type='kubernetes'")

        secure_runtime = app_config.secure_runtime
        if secure_runtime is None or secure_runtime.type != _GVISOR_RUNTIME_TYPE:
            raise ValueError(
                "BatchSandboxProvider requires secure_runtime.type='gvisor'"
            )
        if secure_runtime.k8s_runtime_class != _GVISOR_RUNTIME_CLASS:
            raise ValueError(
                "BatchSandboxProvider requires "
                "secure_runtime.k8s_runtime_class='gvisor'"
            )

        self.k8s_client = k8s_client
        self.ingress_config = app_config.ingress

        k8s_config = app_config.kubernetes
        template_file_path = k8s_config.batchsandbox_template_file if k8s_config else None
        if template_file_path:
            logger.info(f"Using BatchSandbox template file: {template_file_path}")
        self.execd_init_resources = k8s_config.execd_init_resources if k8s_config else None
        self.image_pull_policy = k8s_config.image_pull_policy if k8s_config else "IfNotPresent"
        self.trusted_workloads = _load_trusted_workloads()
        self.trusted_non_fuse_workloads = _load_trusted_workloads(_TRUSTED_NON_FUSE_WORKLOADS_ENV)
        self.trusted_repositories = _load_trusted_repositories()

        self.resolver = SecureRuntimeResolver(app_config)
        self.runtime_class = self.resolver.get_k8s_runtime_class()
        if self.runtime_class != _GVISOR_RUNTIME_CLASS:
            raise ValueError(
                "BatchSandboxProvider requires effective Kubernetes RuntimeClass 'gvisor'"
            )

        self.group = "sandbox.opensandbox.io"
        self.version = "v1alpha1"
        self.plural = "batchsandboxes"

        self.template_manager = BatchSandboxTemplateManager(template_file_path)

    def supports_image_auth(self) -> bool:
        """BatchSandbox supports per-request image pull auth."""
        return True

    def create_workload(
        self,
        sandbox_id: str,
        namespace: str,
        image_spec: ImageSpec,
        entrypoint: List[str],
        env: Dict[str, str],
        resource_limits: Dict[str, str],
        labels: Dict[str, str],
        expires_at: Optional[datetime],
        execd_image: str,
        extensions: Optional[Dict[str, str]] = None,
        network_policy: Optional[NetworkPolicy] = None,
        egress_image: Optional[str] = None,
        volumes: Optional[List[Volume]] = None,
        platform: Optional[PlatformSpec] = None,
        annotations: Optional[Dict[str, str]] = None,
        egress_auth_token: Optional[str] = None,
        egress_mode: str = EGRESS_MODE_DNS,
        credential_proxy_enabled: bool = False,
        resource_requests: Optional[Dict[str, str]] = None,
        egress_env: Optional[Dict[str, Optional[str]]] = None,
    ) -> Dict[str, Any]:
        """Create a gVisor BatchSandbox workload."""
        extensions = extensions or {}
        isolation_enabled = (
            extensions.get(BOOTSTRAP_EXECD_ISOLATION_KEY) == "enable"
        )
        fuse_requested = extensions.get(_FUSE_DEVICE_EXTENSION) == "enable"

        logger.info(
            "Using Kubernetes RuntimeClass '%s' for sandbox %s",
            self.runtime_class,
            sandbox_id,
        )

        if extensions.get("poolRef"):
            raise ValueError(
                "gVisor FUSE-only BatchSandboxProvider does not support pool mode"
            )

        if network_policy is not None or credential_proxy_enabled:
            raise ValueError(
                "gVisor FUSE-only BatchSandboxProvider uses cluster-level egress policy; "
                "request networkPolicy and credentialProxy are not supported"
            )

        if platform is not None and str(platform.os).lower() == "windows":
            raise ValueError(
                "gVisor FUSE-only BatchSandboxProvider supports Linux workloads only"
            )

        # Non-FUSE Environment workers have a separate, explicit exact-image trust
        # grant. Existing FUSE workloads cannot silently lose their FUSE requirement.
        non_fuse_authorized = (
            (image_spec.uri, tuple(entrypoint)) in self.trusted_non_fuse_workloads
        )
        if not isolation_enabled or (not fuse_requested and not non_fuse_authorized):
            raise ValueError(
                "gVisor workloads require bootstrap.execd.isolation=enable "
                "and either orca.fuse.device=enable or an explicitly trusted non-FUSE workload"
            )

        extra_volumes, extra_mounts = self._extract_template_pod_extras()

        init_container = _build_execd_init_container(
            execd_image,
            self.execd_init_resources,
        )

        main_env = dict(env)

        if isolation_enabled and not (
            (non_fuse_authorized and not fuse_requested)
            or (image_spec.uri, tuple(entrypoint)) in self.trusted_workloads
            or (_image_repository(image_spec.uri), tuple(entrypoint))
            in self.trusted_repositories
        ):
            raise ValueError(
                "bootstrap.execd.isolation requested for a workload not authorized by "
                f"{_TRUSTED_REPOSITORY_PREFIXES_ENV}, {_TRUSTED_REPOSITORIES_ENV} "
                f"or {_TRUSTED_WORKLOADS_ENV}"
            )

        main_container = _build_main_container(
            image_spec=image_spec,
            entrypoint=entrypoint,
            env=main_env,
            resource_limits=resource_limits,
            has_network_policy=network_policy is not None,
            isolation_enabled=isolation_enabled,
            image_pull_policy=self.image_pull_policy,
            resource_requests=resource_requests or None,
        )
        
        containers = [_container_to_dict(main_container)]
        pod_volumes = [
            {
                "name": "opensandbox-bin",
                "emptyDir": {}
            }
        ]
        if isolation_enabled:
            pod_volumes.append({
                "name": "isolation-upper",
                "emptyDir": {}
            })
        pod_spec = {
            "automountServiceAccountToken": False,
            "initContainers": [_container_to_dict(init_container)],
            "containers": containers,
            "volumes": pod_volumes,
            "runtimeClassName": _GVISOR_RUNTIME_CLASS,
        }

        self._apply_platform_node_selector(pod_spec, platform)

        if image_spec.auth:
            secret_name = build_image_pull_secret_name(sandbox_id)
            pod_spec["imagePullSecrets"] = [{"name": secret_name}]

        if volumes:
            apply_volumes_to_pod_spec(pod_spec, volumes)

        spec: Dict[str, Any] = {
            "replicas": 1,
            "template": {
                "metadata": {
                    "labels": labels,
                    "annotations": annotations or {},
                },
                "spec": pod_spec,
            },
        }

        runtime_manifest = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "BatchSandbox",
            "metadata": {
                "name": sandbox_id,
                "namespace": namespace,
                "labels": labels,
            },
            "spec": spec,
        }
        if annotations:
            runtime_manifest["metadata"]["annotations"] = annotations

        batchsandbox = self.template_manager.merge_with_runtime_values(runtime_manifest)
        if expires_at is None:
            batchsandbox["spec"].pop("expireTime", None)
        else:
            batchsandbox["spec"]["expireTime"] = expires_at.isoformat()
        self._merge_pod_spec_extras(batchsandbox, extra_volumes, extra_mounts)
        merged_pod_spec = batchsandbox.get("spec", {}).get("template", {}).get("spec", {})
        self._validate_gvisor_fuse_pod_spec(
            merged_pod_spec,
            isolation_enabled=isolation_enabled,
        )
        if platform is not None:
            WorkloadProvider.ensure_platform_compatible_with_affinity(merged_pod_spec, platform)

        created = self.k8s_client.create_custom_object(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
            body=batchsandbox,
        )

        if image_spec.auth:
            secret = build_image_pull_secret(
                sandbox_id=sandbox_id,
                image_uri=image_spec.uri,
                auth=image_spec.auth,
                owner_uid=created["metadata"]["uid"],
                owner_api_version=f"{self.group}/{self.version}",
                owner_kind="BatchSandbox",
            )
            try:
                self.k8s_client.create_secret(namespace=namespace, body=secret)
                logger.info(f"Created imagePullSecret for sandbox {sandbox_id}")
            except Exception:
                logger.warning(f"Failed to create imagePullSecret for sandbox {sandbox_id}, rolling back BatchSandbox")
                try:
                    self.k8s_client.delete_custom_object(
                        group=self.group,
                        version=self.version,
                        namespace=namespace,
                        plural=self.plural,
                        name=sandbox_id,
                        grace_period_seconds=0,
                    )
                except Exception as del_exc:
                    logger.warning(f"Failed to rollback BatchSandbox {sandbox_id}: {del_exc}")
                raise

        return {
            "name": created["metadata"]["name"],
            "uid": created["metadata"]["uid"],
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "BatchSandbox",
        }

    def _apply_platform_node_selector(
        self,
        pod_spec: Dict[str, Any],
        platform: Optional[PlatformSpec],
    ) -> None:
        if platform is None:
            return

        template = self.template_manager.get_base_template()
        template_spec = (
            template.get("spec", {})
            .get("template", {})
            .get("spec", {})
        )
        WorkloadProvider.apply_platform_node_selector(
            pod_spec=pod_spec,
            template_spec=template_spec if isinstance(template_spec, dict) else {},
            platform=platform,
        )

    @staticmethod
    def _validate_gvisor_fuse_pod_spec(
        pod_spec: Dict[str, Any],
        *,
        isolation_enabled: bool,
    ) -> None:
        """Reject template fields that weaken the gVisor-only Pod contract."""
        if pod_spec.get("runtimeClassName") != _GVISOR_RUNTIME_CLASS:
            raise ValueError("sandbox Pod runtimeClassName must be 'gvisor'")

        if "hostUsers" in pod_spec:
            raise ValueError("sandbox Pod must not set hostUsers")
        for field in ("hostNetwork", "hostPID", "hostIPC"):
            if pod_spec.get(field) is True:
                raise ValueError(f"sandbox Pod must not set {field}=true")

        volumes = pod_spec.get("volumes", []) or []
        if any(isinstance(volume, dict) and "hostPath" in volume for volume in volumes):
            raise ValueError("sandbox Pod must not use hostPath volumes")

        sandbox_container = None
        for field in ("initContainers", "containers", "ephemeralContainers"):
            containers = pod_spec.get(field, []) or []
            if not isinstance(containers, list):
                continue
            for container in containers:
                if not isinstance(container, dict):
                    continue
                if field == "containers" and container.get("name") == "sandbox":
                    sandbox_container = container

                for mount in container.get("volumeMounts", []) or []:
                    if isinstance(mount, dict) and mount.get("mountPath") == "/dev/fuse":
                        raise ValueError("sandbox Pod must use gVisor in-sandbox /dev/fuse")

                security_context = container.get("securityContext", {}) or {}
                if not isinstance(security_context, dict):
                    continue
                if security_context.get("privileged") is True:
                    raise ValueError("sandbox Pod containers must not be privileged")
                if "procMount" in security_context:
                    raise ValueError("sandbox Pod containers must not set procMount")

                added = (
                    (security_context.get("capabilities", {}) or {}).get("add", [])
                    or []
                )
                allowed = (
                    {"SETFCAP", "SYS_ADMIN"}
                    if isolation_enabled
                    and field == "containers"
                    and container.get("name") == "sandbox"
                    else set()
                )
                if set(added) != allowed:
                    raise ValueError(
                        "sandbox Pod may add only SETFCAP and SYS_ADMIN to the isolated "
                        "sandbox container"
                    )

        if isolation_enabled and sandbox_container is None:
            raise ValueError("sandbox Pod is missing the sandbox container")

    def _extract_template_pod_extras(self) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
        """Extract extra template volumes and mounts for runtime merge."""
        template = self.template_manager.get_base_template()
        spec = template.get("spec", {}) if isinstance(template, dict) else {}
        template_spec = spec.get("template", {}).get("spec", {})
        extra_volumes = template_spec.get("volumes", []) or []

        extra_mounts: list[Dict[str, Any]] = []
        containers = template_spec.get("containers", []) or []
        if containers:
            target = None
            for container in containers:
                if container.get("name") == "sandbox":
                    target = container
                    break
            if target is None:
                target = containers[0]
            extra_mounts = target.get("volumeMounts", []) or []

        if not isinstance(extra_volumes, list):
            extra_volumes = []
        if not isinstance(extra_mounts, list):
            extra_mounts = []
        return extra_volumes, extra_mounts

    def _merge_pod_spec_extras(
        self,
        batchsandbox: Dict[str, Any],
        extra_volumes: list[Dict[str, Any]],
        extra_mounts: list[Dict[str, Any]],
    ) -> None:
        """Merge template-provided volumes and mounts into runtime pod spec."""
        try:
            spec = batchsandbox["spec"]["template"]["spec"]
        except KeyError:
            return

        volumes = spec.get("volumes", []) or []
        if isinstance(volumes, list) and extra_volumes:
            existing = {v.get("name") for v in volumes if isinstance(v, dict)}
            for vol in extra_volumes:
                if not isinstance(vol, dict):
                    continue
                name = vol.get("name")
                if not name or name in existing:
                    continue
                volumes.append(vol)
                existing.add(name)
            spec["volumes"] = volumes

        containers = spec.get("containers", []) or []
        if not containers or not isinstance(containers, list):
            return
        main_container = containers[0]
        mounts = main_container.get("volumeMounts", []) or []
        if isinstance(mounts, list) and extra_mounts:
            existing = {m.get("name") for m in mounts if isinstance(m, dict)}
            for mnt in extra_mounts:
                if not isinstance(mnt, dict):
                    continue
                name = mnt.get("name")
                if not name or name in existing:
                    continue
                mounts.append(mnt)
                existing.add(name)
            main_container["volumeMounts"] = mounts

    def get_workload(self, sandbox_id: str, namespace: str) -> Optional[Dict[str, Any]]:
        """Get BatchSandbox by sandbox ID."""
        workload = self.k8s_client.get_custom_object(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
            name=sandbox_id,
        )
        if workload:
            return workload

        legacy_name = self.legacy_resource_name(sandbox_id)
        if legacy_name != sandbox_id:
            return self.k8s_client.get_custom_object(
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=self.plural,
                name=legacy_name,
            )

        return None
    
    def delete_workload(self, sandbox_id: str, namespace: str) -> None:
        """Delete BatchSandbox workload."""
        batchsandbox = self.get_workload(sandbox_id, namespace)
        if not batchsandbox:
            raise Exception(f"BatchSandbox for sandbox {sandbox_id} not found")

        self.k8s_client.delete_custom_object(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
            name=batchsandbox["metadata"]["name"],
            grace_period_seconds=0,
        )

    def list_workloads(self, namespace: str, label_selector: str) -> List[Dict[str, Any]]:
        """List BatchSandboxes matching label selector."""
        return self.k8s_client.list_custom_objects(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
            label_selector=label_selector,
        )

    def patch_workload(self, sandbox_id: str, namespace: str, spec_patch: Dict[str, Any]) -> Dict[str, Any]:
        """Patch a mutable BatchSandbox field."""
        batchsandbox = self.get_workload(sandbox_id, namespace)
        if not batchsandbox:
            return None
        return self.k8s_client.patch_custom_object(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
            name=batchsandbox["metadata"]["name"],
            body=spec_patch,
        )

    @staticmethod
    def _first_true_condition_message(conditions: List[Dict[str, Any]], condition_types: List[str]) -> Optional[str]:
        for condition_type in condition_types:
            for cond in conditions:
                if cond.get("type") == condition_type and cond.get("status") == "True":
                    message = cond.get("message")
                    if isinstance(message, str) and message.strip():
                        return message
        return None

    def update_expiration(self, sandbox_id: str, namespace: str, expires_at: datetime) -> None:
        """Update BatchSandbox `spec.expireTime`."""
        batchsandbox = self.get_workload(sandbox_id, namespace)
        if not batchsandbox:
            raise Exception(f"BatchSandbox for sandbox {sandbox_id} not found")

        body = {
            "spec": {
                "expireTime": expires_at.isoformat()
            }
        }
        
        self.k8s_client.patch_custom_object(
            group=self.group,
            version=self.version,
            namespace=namespace,
            plural=self.plural,
            name=batchsandbox["metadata"]["name"],
            body=body,
        )

    def pause_sandbox(self, sandbox_id: str, namespace: str) -> None:
        """Reject pause before OpenSandbox can patch the BatchSandbox CR."""
        raise ValueError(
            "OpenSandbox pause is disabled for gVisor in-sandbox FUSE workloads"
        )

    def resume_sandbox(self, sandbox_id: str, namespace: str) -> None:
        """Reject resume before OpenSandbox can patch the BatchSandbox CR."""
        raise ValueError(
            "OpenSandbox resume is disabled for gVisor in-sandbox FUSE workloads"
        )

    def get_expiration(self, workload: Dict[str, Any]) -> Optional[datetime]:
        """Parse expiration timestamp from `spec.expireTime`."""
        spec = workload.get("spec", {})
        expire_time_str = spec.get("expireTime")
        
        if not expire_time_str:
            return None
        
        try:
            return datetime.fromisoformat(expire_time_str.replace('Z', '+00:00'))
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid expireTime format: {expire_time_str}, error: {e}")
            return None

    def _parse_pod_ip(self, workload: Dict[str, Any]) -> Optional[str]:
        """Parse first pod IP from endpoints annotation."""
        annotations = workload.get("metadata", {}).get("annotations", {})
        endpoints_str = annotations.get("sandbox.opensandbox.io/endpoints")
        if not endpoints_str:
            return None
        try:
            endpoints = json.loads(endpoints_str)
            if endpoints and len(endpoints) > 0:
                return endpoints[0]
        except (json.JSONDecodeError, IndexError, TypeError):
            pass
        return None

    def _platform_unschedulable_message_from_selector(self, workload: Dict[str, Any]) -> Optional[str]:
        workload_has_platform_constraints, workload_has_non_platform_constraints = _workload_platform_constraint_scope(
            workload,
            "template",
            self.analyze_platform_constraints_in_pod_spec,
        )
        if not workload_has_platform_constraints:
            return None
        status = workload.get("status", {})
        selector = status.get("selector")
        namespace = workload.get("metadata", {}).get("namespace")
        if not selector or not namespace:
            return None
        try:
            pods = self.k8s_client.list_pods(
                namespace=namespace,
                label_selector=selector,
            )
        except Exception:
            return None

        for pod in pods:
            message = _extract_platform_unschedulable_message_from_pod(
                pod,
                workload_has_platform_constraints,
                workload_has_non_platform_constraints,
                self.is_platform_unschedulable,
            )
            if message:
                return message
        return None

    def get_status(self, workload: Dict[str, Any]) -> Dict[str, Any]:
        """Derive sandbox state from BatchSandbox status and pod readiness."""
        status = workload.get("status", {})
        creation_timestamp = workload.get("metadata", {}).get("creationTimestamp")

        # Phase is authoritative when set (Pausing/Paused/Resuming/Failed)
        phase = status.get("phase", "")
        failed_message = self._first_true_condition_message(
            status.get("conditions", []),
            ["PodFailed", "ResumeFailed", "PauseFailed"],
        )
        phase_map = {
            "Pending": ("Pending", "CREATING", "Sandbox is being created"),
            "Succeed": ("Running", "RUNNING", "Sandbox is running"),
            "Running": ("Running", "RUNNING", "Sandbox is running"),
            "Pausing": ("Pausing", "PAUSING", "Pausing sandbox"),
            "Paused": ("Paused", "PAUSED", "Sandbox is paused"),
            "Resuming": ("Resuming", "RESUMING", "Resuming sandbox"),
            "Failed": ("Failed", "FAILED", failed_message or "Operation failed"),
        }
        if phase in phase_map:
            state, reason, message = phase_map[phase]
            return {
                "state": state,
                "reason": reason,
                "message": message,
                "last_transition_at": creation_timestamp,
            }

        # Fallback: derive from pod state
        replicas = status.get("replicas", 0)
        ready = status.get("ready", 0)
        allocated = status.get("allocated", 0)
        pod_ip = self._parse_pod_ip(workload)

        if ready == 1 and pod_ip:
            state = "Running"
            reason = "POD_READY_WITH_IP"
            message = f"Pod is ready with IP ({ready}/{replicas} ready)"
        elif pod_ip:
            state = "Allocated"
            reason = "IP_ASSIGNED"
            message = f"Pod has IP assigned but not ready ({allocated}/{replicas} allocated, {ready} ready)"
        else:
            unschedulable_message = self._platform_unschedulable_message_from_selector(workload)
            if unschedulable_message:
                state = "Failed"
                reason = "POD_PLATFORM_UNSCHEDULABLE"
                message = unschedulable_message
            else:
                state = "Pending"
                reason = "POD_SCHEDULED" if allocated > 0 else "BATCHSANDBOX_PENDING"
                message = (
                    f"Pod is scheduled but waiting for IP ({allocated}/{replicas} allocated, {ready} ready)"
                    if allocated > 0
                    else "BatchSandbox is pending allocation"
                )

        return {
            "state": state,
            "reason": reason,
            "message": message,
            "last_transition_at": creation_timestamp,
        }
    
    def get_endpoint_info(self, workload: Dict[str, Any], port: int, sandbox_id: str) -> Optional[Endpoint]:
        """Resolve endpoint using gateway ingress or parsed pod IP."""
        if self.ingress_config and self.ingress_config.mode == INGRESS_MODE_GATEWAY:
            return format_ingress_endpoint(self.ingress_config, sandbox_id, port)

        pod_ip = self._parse_pod_ip(workload)
        if not pod_ip:
            return None
        return Endpoint(endpoint=f"{pod_ip}:{port}")

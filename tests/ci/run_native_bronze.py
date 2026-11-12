"""Orchestrate the reviewed Bronze scenario on an authorized, disposable Actions VM."""

import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

HARNESS = "adc2d56803b9493c2e366f9a31cc3b85ad4e9816"
CANDIDATE = "bcea0e946bb7b199e797943a82cfe82b65a89883"
ORIGIN = "https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender.git"
SUBNET = ipaddress.ip_network("172.30.233.0/28")
REVIEWED_MINIO_IMAGE = "quay.io/minio/minio:RELEASE.2025-07-23T15-54-02Z"
MINIO_DOCKERFILE = "minio-fixture.Dockerfile"
FIXTURES = {
    "fixture-minio": ("objects", "/data", "172.30.233.2"),
    "fixture-hms": ("metastore", "/native-meta", "172.30.233.3"),
}
GIB = 1024**3
LIMITS = {"fixture-minio": (0.5, GIB), "fixture-hms": (1, 2 * GIB), "runner": (2, 4 * GIB)}
LOGGING = {"driver": "json-file", "options": {"max-size": "10m", "max-file": "2"}}
LABEL = "com.docker.compose."


class PilotFailure(RuntimeError):
    """A stage or isolation check failed."""


def require(condition, message):
    if not condition:
        raise PilotFailure(message)


def minio_build(project, context):
    return {
        "image": f"{project}-fixture-minio:bronze",
        "build": {"context": str(context), "dockerfile": MINIO_DOCKERFILE},
    }


def verify_models(base, effective, project, minio_context):
    """Accept only the scoped MinIO build and the intended resource limits."""
    require(base["name"] == project and set(base["services"]) == set(LIMITS), "Unexpected Compose project")
    require(set(base["networks"]) == {"isolated"}, "Unexpected Compose networks")
    network = base["networks"]["isolated"]
    require(network["name"] == f"{project}_isolated" and network["internal"] is True
            and not network.get("external") and not network.get("enable_ipv6")
            and network["ipam"]["config"] == [{"subnet": str(SUBNET)}], "Unexpected Compose network settings")
    require(set(base["volumes"]) == {value[0] for value in FIXTURES.values()}, "Unexpected Compose volumes")
    for name, volume in base["volumes"].items():
        require(volume["name"] == f"{project}_{name}" and not volume.get("external")
                and not volume.get("driver_opts"), "Unexpected Compose volume settings")

    for name, (cpu, memory) in LIMITS.items():
        source = base["services"][name]
        applied = effective["services"][name]
        volume, destination, address = FIXTURES.get(name, (None, None, "172.30.233.4"))
        expected_mounts = [("volume", volume, destination)] if volume else []
        mounts = [(item["type"], item["source"], item["target"]) for item in source.get("volumes", [])]
        require(mounts == expected_mounts and set(source["networks"]) == {"isolated"}
                and source["networks"]["isolated"]["ipv4_address"] == address, f"Unexpected {name} mount or address")
        require(not any(source.get(key) for key in ("ports", "privileged", "network_mode", "devices",
                                                    "pid", "ipc", "volumes_from", "cap_add", "extra_hosts")),
                f"Unexpected {name} host access")
        require(float(applied.pop("cpus")) == cpu and int(applied.pop("mem_limit")) == memory
                and applied.pop("logging") == LOGGING, f"Unexpected {name} resource limits")
    require(base["services"]["runner"]["environment"]["NATIVE_ALLOW_WRITES"] == "0"
            and base["services"]["runner"]["profiles"] == ["native"], "Unexpected runner defaults")
    original_minio = base["services"]["fixture-minio"]
    require(original_minio.get("image") == REVIEWED_MINIO_IMAGE and "build" not in original_minio,
            "Unexpected reviewed MinIO source")
    fixture_minio = effective["services"]["fixture-minio"]
    for field, expected in minio_build(project, minio_context).items():
        require(fixture_minio.pop(field, None) == expected, "Unexpected MinIO fixture build or image")
    fixture_minio["image"] = REVIEWED_MINIO_IMAGE
    require(base == effective, "Overlay changed the reviewed Compose topology")


class BronzePilot:
    def __init__(self):
        self.folder = None
        self.project = None
        self.redactions = []
        self.started = False
        self.minio_image_id = None
        self.invocation = "NOT EXECUTED"

    def diagnostic(self, content):
        for value in self.redactions:
            content = content.replace(value, "[redacted]")
        for line in content.splitlines()[-40:]:
            safe = re.sub(r"[^\x20-\x7e]", " ", line).replace("::", ": :").replace("##[", "# #[")
            print(f"  {safe[:350]}", flush=True)

    def command(self, stage, args, *, timeout=30, source=None, diagnostics=False):
        print(f"Stage: {stage}", flush=True)
        try:
            with tempfile.TemporaryFile() as errors:
                result = subprocess.run(args, input=source, stdout=subprocess.PIPE, stderr=errors,
                                        timeout=timeout, check=False)
                if result.returncode:
                    if diagnostics:
                        errors.seek(max(0, errors.tell() - 16384))
                        output = result.stdout[-16384:] + b"\n" + errors.read()
                        self.diagnostic(output.decode(errors="replace"))
                    raise PilotFailure(f"{stage} failed (exit {result.returncode})")
                return result.stdout
        except subprocess.TimeoutExpired:
            raise PilotFailure(f"{stage} timed out after {timeout}s") from None

    def docker(self, stage, *args, **kwargs):
        return self.command(stage, ["docker", *args], **kwargs)

    def compose(self, stage, *args, limited=True, **kwargs):
        options = ["compose", "--parallel", "1", "--project-name", self.project,
                   "--project-directory", str(self.folder), "--env-file", str(self.folder / "test.env"),
                   "-f", str(self.folder / "compose.json")]
        if limited:
            options += ["-f", str(self.overlay)]
        return self.docker(stage, *options, "--profile", "native", *args, **kwargs)

    def check_host(self):
        require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
                and os.environ.get("RUNNER_OS") == "Linux", "Expected a GitHub-hosted Linux runner")
        require(os.environ.get("NATIVE_ALLOW_WRITES") == "0", "Expected disabled writes at job scope")
        require(not any(value for name, value in os.environ.items()
                        if name.startswith("COMPOSE_") or name in
                        {"DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH", "DOCKER_CONFIG",
                         "BUILDKIT_HOST", "BUILDX_BUILDER"}),
                "Unexpected Docker or Compose override")
        context = self.docker("read Docker context", "context", "show").decode().strip()
        endpoint = self.docker("verify Docker endpoint", "context", "inspect", context,
                               "--format", "{{.Endpoints.docker.Host}}").decode().strip()
        require(endpoint == "unix:///var/run/docker.sock", "Expected the local Docker socket")
        docker = json.loads(self.docker("read local Docker information", "info", "--format", "{{json .}}"))
        require(docker["OSType"] == "linux" and docker["Architecture"] in ("x86_64", "amd64"),
                "Expected Linux AMD64 containers")
        version = self.docker("read Compose version", "compose", "version", "--short").decode().strip()
        require(re.match(r"^v?2\.", version), "Expected Compose v2")
        available = next((int(line.split()[1]) * 1024 for line in Path("/proc/meminfo").read_text().splitlines()
                          if line.startswith("MemAvailable:")), 0)
        free = min(shutil.disk_usage(docker["DockerRootDir"]).free, shutil.disk_usage("/tmp").free)
        print(f"Runner Docker {docker['ServerVersion']}, Compose {version}, {docker['NCPU']} CPU, "
              f"available RAM {available} bytes, free Docker/temp disk {free} bytes", flush=True)
        require(docker["NCPU"] >= 2 and available >= 8_000_000_000 and free >= 8 * GIB,
                "Runner is below the initial CPU, RAM or disk budget")

    def prepare(self, minio_context):
        head = os.environ.get("PILOT_HEAD", "")
        require(re.fullmatch(r"[0-9a-f]{40}", head), "Missing approved pilot head")
        pilot = Path(__file__).resolve().parents[2]
        require(self.command("verify pilot checkout", ["git", "-C", str(pilot), "rev-parse", "HEAD"])
                .decode().strip() == head, "Unexpected pilot checkout")
        require(self.command("verify harness checkout", ["git", "rev-parse", "HEAD"])
                .decode().strip() == HARNESS, "Unexpected harness checkout")
        print(f"Pilot {head} | harness {HARNESS} | candidate {CANDIDATE} | scenario bronze", flush=True)
        self.command("fetch reviewed Bronze candidate", ["git", "--no-replace-objects", "-c", "credential.helper=",
                     "fetch", "--no-tags", "--depth=1", ORIGIN, CANDIDATE], timeout=60)
        source = self.command("read reviewed preparation CLI",
                              ["git", "--no-replace-objects", "show", f"{HARNESS}:tests/native/prepare.py"])
        cli = [sys.executable, "-I", "-B", "-"]
        arguments = ["--harness-revision", HARNESS, "--candidate", CANDIDATE, "--scenario", "bronze"]
        folder = Path(self.command("stage synthetic Bronze bundle", [*cli, "prepare", *arguments],
                                   source=source, timeout=60).decode().strip())
        require(folder.parent.resolve() == Path("/tmp") and not folder.is_symlink()
                and re.fullmatch(r"native-[0-9a-f]{16}-[a-z0-9_]+", folder.name),
                "Unexpected disposable bundle path")
        self.folder = folder
        self.project = folder.name.rsplit("-", 1)[0]
        self.command("verify reviewed bundle", [*cli, "verify", *arguments, str(folder)], source=source, timeout=60)
        self.redactions = [line.partition("=")[2] for line in (folder / "test.env").read_text().splitlines()]
        dockerfile = self.command("read approved MinIO fixture Dockerfile",
                                  ["git", "--no-replace-objects", "-C", str(pilot), "show",
                                   f"{head}:tests/ci/{MINIO_DOCKERFILE}"])
        minio_context.mkdir()
        (minio_context / MINIO_DOCKERFILE).write_bytes(dockerfile)
        print("PASS: bundle provenance and synthetic credential scope", flush=True)

    def resources(self):
        project = self.project
        filter_arg = f"label={LABEL}project={project}"
        ids = self.docker("list scoped containers", "container", "ls", "-aq", "--no-trunc",
                          "--filter", filter_arg).decode().split()
        containers = json.loads(self.docker("inspect scoped containers", "container", "inspect", *ids)) if ids else []
        # Match both names and labels so collisions and extra labeled resources are visible.
        names = set(self.docker("list Docker volume names", "volume", "ls", "-q").decode().split())
        labeled = self.docker("list project volume names", "volume", "ls", "-q", "--filter", filter_arg)
        expected = {f"{project}_{item[0]}" for item in FIXTURES.values()}
        volumes = sorted((names & expected) | set(labeled.decode().split()))
        volumes = json.loads(self.docker("inspect scoped volumes", "volume", "inspect", *volumes)) if volumes else []
        network_list = ("network", "ls", "--format", "{{.Name}}")
        names = set(self.docker("list Docker network names", *network_list).decode().split())
        labeled = self.docker("list project network names", *network_list, "--filter", filter_arg)
        networks = sorted((names & {f"{project}_isolated"}) | set(labeled.decode().split()))
        networks = json.loads(self.docker("inspect scoped networks", "network", "inspect", *networks)) if networks else []
        return containers, volumes, networks

    def verify_fresh(self):
        require(not any(self.resources()), "Disposable project already has resources")
        ids = self.docker("list existing network IDs", "network", "ls", "-q").decode().split()
        networks = json.loads(self.docker("inspect existing IPAM", "network", "inspect", *ids)) if ids else []
        routes = json.loads(self.command("inspect local IPv4 routes", ["ip", "-j", "-4", "route", "show", "table", "all"]))
        prefixes = [entry["dst"] for entry in routes if entry.get("dst") not in (None, "default", "0.0.0.0/0")]
        prefixes += [entry["Subnet"] for network in networks for entry in (network.get("IPAM", {}).get("Config") or [])
                     if "Subnet" in entry]
        for prefix in prefixes:
            existing = ipaddress.ip_network(prefix, strict=False)
            require(existing.version != 4 or not existing.overlaps(SUBNET), "Fixture subnet overlaps a local route or IPAM")

    def verify_ownership(self, containers, volumes, networks):
        """Check ownership before using or removing any project resource."""
        for item in containers:
            labels = item["Config"]["Labels"]
            require(labels.get(LABEL + "project") == self.project and labels.get(LABEL + "service") in LIMITS
                    and item["Name"].startswith(f"/{self.project}-"), "Unexpected container in project")
        for item in volumes:
            labels = item.get("Labels") or {}
            volume = labels.get(LABEL + "volume")
            require(volume in {"objects", "metastore"} and labels.get(LABEL + "project") == self.project
                    and item["Name"] == f"{self.project}_{volume}" and item["Driver"] == "local"
                    and not item.get("Options"), "Unexpected volume ownership or driver")
        for item in networks:
            labels = item.get("Labels") or {}
            require(labels.get(LABEL + "project") == self.project and labels.get(LABEL + "network") == "isolated"
                    and item["Name"] == f"{self.project}_isolated"
                    and set(item.get("Containers") or {}) <= {container["Id"] for container in containers},
                    "Unexpected network ownership or endpoint")

    def verify_running(self):
        containers, volumes, networks = self.resources()
        self.verify_ownership(containers, volumes, networks)
        require(len(containers) == len(volumes) == 2 and len(networks) == 1, "Expected exactly two disposable fixtures")
        network = networks[0]
        require(network["Internal"] and network["Driver"] == "bridge" and not network["EnableIPv6"]
                and [item["Subnet"] for item in network["IPAM"]["Config"]] == [str(SUBNET)]
                and set(network["Containers"]) == {item["Id"] for item in containers},
                "Unexpected running network topology")
        require({item["Config"]["Labels"][LABEL + "service"] for item in containers} == set(FIXTURES),
                "Unexpected running fixture services")
        for item in containers:
            service = item["Config"]["Labels"][LABEL + "service"]
            volume, destination, address = FIXTURES[service]
            cpu, memory = LIMITS[service]
            mounts = [(m["Type"], m.get("Name"), m["Destination"]) for m in item["Mounts"]]
            connected = item["NetworkSettings"]["Networks"]
            name = f"{self.project}_isolated"
            require(item["State"]["Running"] and not item["HostConfig"]["Privileged"]
                    and not item["HostConfig"].get("PortBindings")
                    and not any((item["NetworkSettings"].get("Ports") or {}).values()), f"Unexpected {service} state or ports")
            require(mounts == [("volume", f"{self.project}_{volume}", destination)]
                    and set(connected) == {name} and connected[name]["IPAddress"] == address
                    and item["HostConfig"]["Memory"] == memory
                    and item["HostConfig"]["NanoCpus"] == int(cpu * 1_000_000_000),
                    f"Unexpected {service} runtime mount, network or limits")
            if service == "fixture-minio":
                require(item["Image"] == self.minio_image_id, "Running MinIO image differs from the verified build")
            print(f"Verified {service} fixture image {item['Image']}", flush=True)
        print("PASS: isolated network, fixture ownership, volumes and resource limits", flush=True)

    def run(self, work):
        self.check_host()
        minio_context = work / "minio-fixture"
        self.prepare(minio_context)
        self.overlay = work / "fixture-overlay.json"
        services = {name: {"cpus": cpu, "mem_limit": memory, "logging": LOGGING}
                    for name, (cpu, memory) in LIMITS.items()}
        services["fixture-minio"].update(minio_build(self.project, minio_context))
        self.overlay.write_text(json.dumps({"services": services}))
        config = ["config", "--no-env-resolution", "--format", "json"]
        base = json.loads(self.compose("resolve reviewed Compose model", *config, limited=False))
        effective = json.loads(self.compose("resolve bounded Compose model", *config))
        verify_models(base, effective, self.project, minio_context)
        self.verify_fresh()
        self.compose("build runner and Hive images serially", "build", "--builder", "default", "fixture-hms", "runner",
                     timeout=900, diagnostics=True)
        runner_image = self.docker("read built runner image ID", "image", "inspect", f"{self.project}-runner",
                                   "--format", "{{.Id}}").decode().strip()
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", runner_image), "Unexpected runner image ID")
        print(f"Verified runner image {runner_image}", flush=True)
        self.compose("build checksum-pinned MinIO fixture", "build", "--builder", "default", "fixture-minio",
                     timeout=180, diagnostics=True)
        self.minio_image_id = self.docker("read built MinIO image ID", "image", "inspect",
                                         minio_build(self.project, minio_context)["image"],
                                         "--format", "{{.Id}}").decode().strip()
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", self.minio_image_id), "Unexpected MinIO image ID")
        self.started = True
        self.compose("start only MinIO and Hive fixtures", "up", "--wait", "--wait-timeout", "90",
                     "--no-build", "--pull", "never", *FIXTURES, timeout=120, diagnostics=True)
        self.verify_running()
        self.invocation = "FAIL"
        output = self.compose("run bounded native Bronze", "run", "--pull", "never", "--rm", "--no-deps",
                              "-T", "--interactive=false", "-e", "NATIVE_ALLOW_WRITES=1", "runner",
                              timeout=300, diagnostics=True)
        require(b"Scenario completed: bronze" in output, "Bronze scenario completion marker missing")
        checksum = re.search(rb"Bronze final object SHA-256:\s*([0-9a-f]{64})", output)
        require(checksum, "Bronze scenario checksum missing")
        self.invocation = "PASS"
        print(f"PASS: Bronze assertions completed, final object SHA-256 {checksum[1].decode()}", flush=True)

    def cleanup_project(self):
        containers, volumes, networks = self.resources()
        self.verify_ownership(containers, volumes, networks)
        if containers:
            self.docker("remove verified project containers", "container", "rm", "-f",
                        *(item["Id"] for item in containers))
        if networks:
            self.docker("remove verified project network", "network", "rm", *(item["Id"] for item in networks))
        if volumes:
            self.docker("remove verified project volumes", "volume", "rm", *(item["Name"] for item in volumes))
        require(not any(self.resources()), "Project cleanup is incomplete")
        print("PASS: scoped project cleanup", flush=True)

    def cleanup(self):
        try:
            if self.started:
                self.cleanup_project()
        finally:
            if self.folder:
                require(not self.folder.is_symlink() and self.folder.parent.resolve() == Path("/tmp")
                        and re.fullmatch(r"native-[0-9a-f]{16}-[a-z0-9_]+", self.folder.name),
                        "Unexpected bundle path during cleanup")
                shutil.rmtree(self.folder)


def main():
    pilot = BronzePilot()
    failure = None
    cleanup_failure = None
    with tempfile.TemporaryDirectory(prefix="bronze-pilot-") as work:
        try:
            pilot.run(Path(work))
        except PilotFailure as error:
            failure = str(error)
            print(f"FAIL: {failure}", flush=True)
        except (OSError, ValueError, KeyError, TypeError) as error:
            failure = f"Unexpected {type(error).__name__}; see the preceding stage"
            print(f"FAIL: {failure}", flush=True)
        finally:
            try:
                pilot.cleanup()
            except (PilotFailure, OSError, ValueError, KeyError, TypeError) as error:
                cleanup_failure = str(error) if isinstance(error, PilotFailure) else type(error).__name__
                print(f"FAIL: scoped cleanup: {cleanup_failure}", flush=True)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        head = os.environ.get("PILOT_HEAD", "")
        if not re.fullmatch(r"[0-9a-f]{40}", head):
            head = "unavailable"
        summary.write(f"## Bounded Bronze validation\n\n- Pilot head: {head}\n"
                      f"- Harness: {HARNESS}\n- Candidate: {CANDIDATE}\n"
                      f"- Native invocation: {pilot.invocation}\n")
        summary.write(f"- Result: {'FAIL' if failure or cleanup_failure else 'PASS'}\n")
        if failure:
            summary.write(f"- First failure: {failure}\n")
        if cleanup_failure:
            summary.write(f"- Cleanup failure: {cleanup_failure}\n")
    return int(bool(failure or cleanup_failure))


if __name__ == "__main__":
    sys.exit(main())

"""pytest fixtures: Docker access and the stand-in replica image (SIMULATED, no model)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # so tests can import gateway.*
from helpers import ROOT, STANDIN_IMAGE, TEST_PREFIX  # noqa: E402


@pytest.fixture(scope="session")
def docker_client():
    docker = pytest.importorskip("docker")
    try:
        client = docker.from_env()
        client.ping()
    except Exception as e:  # pragma: no cover - environment dependent
        pytest.skip(f"Docker not reachable: {e}")
    return client


@pytest.fixture(scope="session")
def standin_image(docker_client):
    subprocess.run(["docker", "build", "-q", "-t", STANDIN_IMAGE, "-f", "tests/standin.Dockerfile", "tests"],
                   cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
    return STANDIN_IMAGE


@pytest.fixture
def no_leftover_containers(docker_client):
    """Fails the test if any test replica container is still around afterwards."""
    yield
    left = [c.name for c in docker_client.containers.list(all=True) if c.name.startswith(TEST_PREFIX)]
    for name in left:
        docker_client.containers.get(name).remove(force=True)
    assert not left, f"containers left behind: {left}"

# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Exercise independent qemu-img QCOW2 images through real guest block I/O."""

import hashlib
import json
import os
from pathlib import Path

import pytest

from framework.artifacts import GUEST_KERNELS_5_10, pin_guest_kernel
from framework.utils import check_output, get_threads, run_cmd
from framework.utils_vsock import (
    ECHO_SERVER_PORT,
    VSOCK_UDS_PATH,
    start_guest_echo_server,
    vsock_connect_to_guest,
)
from host_tools.cargo_build import run_seccompiler_bin

pytestmark = pin_guest_kernel(GUEST_KERNELS_5_10[:1])

CLUSTER_SIZE = 64 * 1024
DISK_SIZE = 4 * 1024 * 1024
GUEST_DATA = "/dev/shm/qcow2-data"


def _convert(source, target, *, compressed=False):
    """Generate a self-contained image without using the implementation under test."""
    check_output(
        [
            "qemu-img",
            "convert",
            *(["-c"] if compressed else []),
            "-f",
            "raw",
            "-O",
            "qcow2",
            "-o",
            f"cluster_size={CLUSTER_SIZE},compat=1.1",
            str(source),
            str(target),
        ]
    )


def _image(vm, name, seed=0, *, compressed=False):
    """Create allocated nonzero clusters, a sparse middle, and a nonzero last sector."""
    raw = Path(vm.fsfiles) / f"{name}.raw"
    image = raw.with_suffix(".qcow2")
    pattern = bytes((index + seed) % 255 + 1 for index in range(255))
    allocated = (pattern * (2 * CLUSTER_SIZE // len(pattern) + 1))[: 2 * CLUSTER_SIZE]
    expected = bytearray(DISK_SIZE)
    expected[: len(allocated)] = allocated
    expected[-512:] = (pattern * 3)[:512]
    with raw.open("wb") as output:
        output.truncate(DISK_SIZE)
        output.write(allocated)
        output.seek(DISK_SIZE - 512)
        output.write(expected[-512:])
    _convert(raw, image, compressed=compressed)
    return image, expected


def _digest(data):
    return hashlib.sha256(data).hexdigest()


def _assert_guest_data(vm, expected, device="/dev/vdb"):
    """Read every sector directly, also leaving device-written dirty memory in tmpfs."""
    _, stdout, _ = vm.ssh.check_output(
        f"dd if={device} of={GUEST_DATA} bs=65536 iflag=direct status=none"
        f" && sha256sum {GUEST_DATA}"
    )
    assert stdout.split()[0] == _digest(expected)


def _write_pattern(
    vm, expected, offset, length, seed, *, flush=True
):  # pylint: disable=too-many-arguments
    """Write nonzero sector-aligned data, including allocations within sparse clusters."""
    payload = bytes((index + seed) % 251 + 1 for index in range(length))
    source = Path(vm.fsfiles) / "qcow2-write.bin"
    source.write_bytes(payload)
    vm.ssh.scp_put(str(source), "/dev/shm/qcow2-write")
    conv = "notrunc,fsync" if flush else "notrunc"
    vm.ssh.check_output(
        "dd if=/dev/shm/qcow2-write of=/dev/vdb bs=512"
        f" seek={offset // 512} count={length // 512}"
        f" oflag=direct conv={conv} status=none"
    )
    expected[offset : offset + length] = payload


def _check_closed_image(image, expected):
    """Validate metadata and logical contents after all Firecracker handles are closed."""
    check_output(["qemu-img", "check", "-f", "qcow2", str(image)])
    raw = image.with_suffix(".checked.raw")
    check_output(
        ["qemu-img", "convert", "-f", "qcow2", "-O", "raw", str(image), str(raw)]
    )
    assert _digest(raw.read_bytes()) == _digest(expected)


def _assert_queue_mode(vm, num_queues, drive_id="qcow2"):
    assert (f"fc_q0_{drive_id}" in get_threads(vm.firecracker_pid)) == (
        num_queues is not None
    )


def _assert_readonly_multiqueue(vm, expected):
    """Verify shared read-only data, queue workers, and negotiated write protection."""
    _assert_queue_mode(vm, 2)
    assert "fc_q1_qcow2" in get_threads(vm.firecracker_pid)
    _assert_queue_mode(vm, 1, drive_id="shared")
    for device in ["/dev/vdc", "/dev/vdd"]:
        _assert_guest_data(vm, expected, device=device)
        _, stdout, _ = vm.ssh.check_output(f"blockdev --getro {device}")
        assert int(stdout) == 1
        rc, _, stderr = vm.ssh.run(
            f"dd if=/dev/zero of={device} bs=512 count=1 oflag=direct status=none"
        )
        assert rc != 0
        assert "Operation not permitted" in stderr


@pytest.mark.parametrize("compressed", [False, True])
def test_qcow2_writable_io(uvm, num_queues, compressed):
    """Direct writes preserve ordinary and compressed images across partial-cluster COW."""
    uvm.spawn()
    uvm.basic_config()
    uvm.add_net_iface()
    image, expected = _image(uvm, "writable", compressed=compressed)
    uvm.add_drive(
        "qcow2",
        image,
        image_format="Qcow2",
        num_queues=num_queues,
        cache_type="Writeback",
    )
    uvm.start()
    _assert_queue_mode(uvm, num_queues)
    _, stdout, _ = uvm.ssh.check_output("blockdev --getsize64 /dev/vdb")
    assert int(stdout) == DISK_SIZE
    _assert_guest_data(uvm, expected)

    for iteration in range(3):
        _write_pattern(
            uvm, expected, CLUSTER_SIZE - 512, 2 * CLUSTER_SIZE + 1024, iteration * 31
        )
        _write_pattern(
            uvm, expected, (8 + iteration) * CLUSTER_SIZE + 512, 4096, iteration * 47
        )
        _write_pattern(uvm, expected, DISK_SIZE - 4096, 4096, iteration * 59)
        _assert_guest_data(uvm, expected)

    uvm.kill()
    _check_closed_image(image, expected)


def test_qcow2_live_truncation(uvm, num_queues):
    """Missing allocated bytes fail guest I/O without changing valid or sparse reads."""
    uvm.spawn()
    uvm.basic_config()
    uvm.add_net_iface()
    image, expected = _image(uvm, "truncated")
    _, stdout, _ = check_output(
        ["qemu-img", "map", "--output=json", "-f", "qcow2", str(image)]
    )
    tail = next(
        extent
        for extent in json.loads(stdout)
        if extent["start"] <= DISK_SIZE - 512 < extent["start"] + extent["length"]
    )
    assert tail["data"] and tail["present"]
    host_offset = tail["offset"] + DISK_SIZE - 512 - tail["start"]
    uvm.add_drive("qcow2", image, image_format="Qcow2", num_queues=num_queues)
    uvm.start()
    _assert_queue_mode(uvm, num_queues)
    _assert_guest_data(uvm, expected)

    for remaining in [511, 0]:
        os.truncate(image, host_offset + remaining)
        rc, _, stderr = uvm.ssh.run(
            "dd if=/dev/vdb of=/dev/shm/truncated-read bs=512"
            f" skip={DISK_SIZE // 512 - 1} count=1 iflag=direct status=none"
        )
        assert rc != 0
        assert "Input/output error" in stderr
        _, stdout, _ = uvm.ssh.check_output(
            "dd if=/dev/vdb bs=512 count=1 iflag=direct status=none | sha256sum"
        )
        assert stdout.split()[0] == _digest(expected[:512])
        _, stdout, _ = uvm.ssh.check_output(
            "dd if=/dev/vdb bs=512 skip=1024 count=1 iflag=direct status=none"
            " | sha256sum"
        )
        assert stdout.split()[0] == _digest(bytes(512))


def test_qcow2_unclean_shutdown(uvm, num_queues):
    """Published L2 tables retain their refcounts after Firecracker is killed."""
    image = Path(uvm.fsfiles) / "unclean.qcow2"
    check_output(
        [
            "qemu-img",
            "create",
            "-f",
            "qcow2",
            "-o",
            "cluster_size=512",
            str(image),
            "4M",
        ]
    )
    uvm.spawn()
    uvm.basic_config()
    uvm.add_net_iface()
    uvm.add_drive("qcow2", image, image_format="Qcow2", num_queues=num_queues)
    uvm.start()
    expected = bytearray(DISK_SIZE)
    for offset in (512, 2 * 1024 * 1024 - 512, 3 * 1024 * 1024):
        _write_pattern(uvm, expected, offset, 65536, 73, flush=False)
    uvm.kill()

    result = run_cmd(["qemu-img", "check", "-f", "qcow2", "--output=json", str(image)])
    # Unflushed writes can leak clusters, but must not publish unowned metadata.
    assert result.returncode in (0, 3), (result.stdout, result.stderr)
    assert json.loads(result.stdout).get("corruptions", 0) == 0


def test_qcow2_snapshot_sync_failure(uvm, num_queues):
    """A failed disk sync rejects snapshots without losing the paused device or its data."""
    filter_path = Path(uvm.fsfiles) / "sync-error.json"
    filter_path.write_text(
        json.dumps(
            {
                thread: {
                    "default_action": "allow",
                    "filter_action": {"errno": 5},
                    "filter": [{"syscall": "fsync"}],
                }
                for thread in ["vmm", "api", "vcpu", "blk_worker"]
            }
        )
    )
    bpf_path = filter_path.with_suffix(".bpf")
    run_seccompiler_bin(bpf_path=bpf_path, json_path=filter_path)
    uvm.create_jailed_resource(bpf_path)
    uvm.jailer.extra_args["seccomp-filter"] = bpf_path.name
    uvm.spawn()
    uvm.basic_config()
    uvm.add_net_iface()
    image, expected = _image(uvm, "sync-error")
    uvm.add_drive(
        "qcow2",
        image,
        image_format="Qcow2",
        num_queues=num_queues,
        cache_type="Writeback",
    )
    uvm.api.vsock.put(vsock_id="vsock0", guest_cid=3, uds_path="/" + VSOCK_UDS_PATH)
    uvm.start()
    _assert_queue_mode(uvm, num_queues)
    uds_path = start_guest_echo_server(uvm)
    with vsock_connect_to_guest(uds_path, ECHO_SERVER_PORT) as connection:
        connection.settimeout(5)
        connection.sendall(b"x")
        assert connection.recv(1) == b"x"
        for attempt in range(2):
            _write_pattern(
                uvm, expected, (6 + attempt) * CLUSTER_SIZE + 512, 4096, 23, flush=False
            )
            uvm.pause()
            with pytest.raises(RuntimeError, match="QCOW2.*flush|QCOW2.*sync"):
                uvm.api.snapshot_create.put(
                    mem_file_path="failed_mem",
                    snapshot_path="failed_state",
                    snapshot_type="Full",
                    sync_snapshot_files=False,
                )
            assert not (Path(uvm.chroot()) / "failed_state").exists()
            uvm.resume()
            _assert_guest_data(uvm, expected)
            connection.sendall(b"x")
            assert connection.recv(1) == b"x"
    uvm.kill()
    _check_closed_image(image, expected)


@pytest.mark.parametrize("cache_type", ["Unsafe", "Writeback"])
def test_qcow2_full_and_diff_snapshot(uvm, microvm_factory, num_queues, cache_type):
    """Full and diff restoration preserve disk data, dirty guest reads, and writable I/O."""
    uvm.spawn()
    uvm.basic_config(track_dirty_pages=True)
    uvm.add_net_iface()
    image, expected = _image(uvm, "snapshot")
    uvm.add_drive(
        "qcow2",
        image,
        image_format="Qcow2",
        num_queues=num_queues,
        cache_type=cache_type,
    )
    uvm.start()
    _write_pattern(uvm, expected, 6 * CLUSTER_SIZE + 512, 4096, 23, flush=False)
    _assert_guest_data(uvm, expected)
    full = uvm.snapshot_full(mem_path="full_mem", vmstate_path="full_state")
    uvm.kill()
    _check_closed_image(image, expected)

    restored = microvm_factory.build(pci=uvm.pci_enabled)
    restored.spawn()
    restored.restore_from_snapshot(full, resume=True)
    _assert_queue_mode(restored, num_queues)
    _, stdout, _ = restored.ssh.check_output(f"sha256sum {GUEST_DATA}")
    assert stdout.split()[0] == _digest(expected)
    _assert_guest_data(restored, expected)

    base = restored.snapshot_full(mem_path="base_mem", vmstate_path="base_state")
    restored.resume()
    _write_pattern(
        restored,
        expected,
        12 * CLUSTER_SIZE - 512,
        CLUSTER_SIZE + 1024,
        79,
        flush=False,
    )
    _assert_guest_data(restored, expected)
    diff = restored.snapshot_diff(mem_path="diff_mem", vmstate_path="diff_state")
    restored.kill()
    _check_closed_image(image, expected)
    snapshot = diff.rebase_snapshot(
        base, use_snapshot_editor=True, binary_dir=uvm.fc_binary_path.parent
    )
    resumed = microvm_factory.build(pci=uvm.pci_enabled)
    resumed.spawn()
    resumed.restore_from_snapshot(snapshot, resume=True)
    _assert_queue_mode(resumed, num_queues)
    _, stdout, _ = resumed.ssh.check_output(f"sha256sum {GUEST_DATA}")
    assert stdout.split()[0] == _digest(expected)
    _assert_guest_data(resumed, expected)
    _write_pattern(resumed, expected, 20 * CLUSTER_SIZE + 512, 4096, 131)
    _assert_guest_data(resumed, expected)
    resumed.snapshot_full()
    resumed.kill()
    _check_closed_image(image, expected)


def test_qcow2_patch_dirty_image(uvm, num_queues):
    """PATCH keeps dirty state on no-op/error and flushes the old image before replacing it."""
    uvm.spawn()
    uvm.basic_config()
    uvm.add_net_iface()
    original, original_data = _image(uvm, "original")
    replacement, replacement_data = _image(uvm, "replacement", seed=83)
    alias = Path(uvm.fsfiles) / "original-alias.qcow2"
    os.link(original, alias)
    invalid = Path(uvm.fsfiles) / "invalid.qcow2"
    invalid.write_bytes(b"not a QCOW2 image" * 256)
    malformed, _ = _image(uvm, "short-l1")
    malformed_data = bytearray(malformed.read_bytes())
    l1_size = int.from_bytes(malformed_data[36:40], "big")
    malformed_data[24:32] = (l1_size * CLUSTER_SIZE**2 // 8 + 512).to_bytes(8, "big")
    malformed_data[88:96] = (1 << 63).to_bytes(8, "big")
    malformed.write_bytes(malformed_data)
    for read_only in [False, True]:
        with pytest.raises(RuntimeError, match="L1 table"):
            uvm.add_drive(
                "malformed",
                malformed,
                image_format="Qcow2",
                is_read_only=read_only,
            )
        assert malformed.read_bytes() == malformed_data
    uvm.add_drive(
        "qcow2",
        original,
        image_format="Qcow2",
        num_queues=num_queues,
        cache_type="Writeback",
    )
    uvm.start()
    _write_pattern(uvm, original_data, 8 * CLUSTER_SIZE + 512, 4096, 19, flush=False)
    threads = get_threads(uvm.firecracker_pid).get("fc_q0_qcow2")
    uvm.api.drive.patch(
        drive_id="qcow2", path_on_host=uvm.create_jailed_resource(alias)
    )
    assert get_threads(uvm.firecracker_pid).get("fc_q0_qcow2") == threads
    _assert_guest_data(uvm, original_data)

    for rejected in [invalid, malformed]:
        with pytest.raises(RuntimeError, match="QCOW2|qcow2"):
            uvm.api.drive.patch(
                drive_id="qcow2", path_on_host=uvm.create_jailed_resource(rejected)
            )
        _assert_guest_data(uvm, original_data)
    assert malformed.read_bytes() == malformed_data
    _write_pattern(uvm, original_data, 10 * CLUSTER_SIZE - 512, 8192, 41, flush=False)
    uvm.api.drive.patch(
        drive_id="qcow2", path_on_host=uvm.create_jailed_resource(replacement)
    )
    _assert_queue_mode(uvm, num_queues)
    _assert_guest_data(uvm, replacement_data)
    _write_pattern(uvm, replacement_data, 16 * CLUSTER_SIZE + 512, 4096, 67)
    _assert_guest_data(uvm, replacement_data)
    uvm.kill()
    _check_closed_image(original, original_data)
    _check_closed_image(replacement, replacement_data)


def test_qcow2_readonly_multiqueue_and_invalid_config(uvm, microvm_factory):
    """Shared read-only queues work; unsafe configuration and multi-queue PATCH fail."""
    uvm.spawn()
    uvm.basic_config()
    uvm.add_net_iface()
    image, expected = _image(uvm, "readonly")
    writer, writer_data = _image(uvm, "writer", seed=17)
    image_alias = Path(uvm.fsfiles) / "readonly-alias.qcow2"
    os.link(image, image_alias)
    os.link(writer, writer.with_name("writer-alias.qcow2"))

    for drive_id, options, message in [
        ("discard", {"discard": True}, "discard.*not supported"),
        ("async", {"io_engine": "Async"}, "async file engine"),
        ("writable_mq", {"num_queues": 2}, "multiple queues"),
    ]:
        with pytest.raises(RuntimeError, match=message):
            uvm.add_drive(drive_id, image, image_format="Qcow2", **options)

    uvm.add_drive("writer", writer, image_format="Qcow2")
    for read_only in [False, True]:
        with pytest.raises(RuntimeError, match="lock|busy|already"):
            uvm.add_drive(
                "duplicate",
                writer.with_name("writer-alias.qcow2"),
                image_format="Qcow2",
                is_read_only=read_only,
            )
    uvm.add_drive("qcow2", image, image_format="Qcow2", is_read_only=True, num_queues=2)
    uvm.add_drive(
        "shared", image_alias, image_format="Qcow2", is_read_only=True, num_queues=1
    )
    with pytest.raises(RuntimeError, match="lock|busy|already"):
        uvm.add_drive("duplicate", image_alias, image_format="Qcow2")
    uvm.start()
    for _ in range(3):
        _assert_readonly_multiqueue(uvm, expected)
    replacement, replacement_data = _image(uvm, "readonly-replacement", seed=91)
    with pytest.raises(RuntimeError, match="multiple queues|multi-queue"):
        uvm.api.drive.patch(
            drive_id="qcow2", path_on_host=uvm.create_jailed_resource(replacement)
        )
    _assert_guest_data(uvm, expected, device="/dev/vdc")
    snapshot = uvm.snapshot_full()
    uvm.kill()
    _check_closed_image(image, expected)
    _check_closed_image(writer, writer_data)
    _check_closed_image(replacement, replacement_data)
    restored = microvm_factory.build(pci=uvm.pci_enabled)
    restored.spawn()
    restored.restore_from_snapshot(snapshot, resume=True)
    _assert_readonly_multiqueue(restored, expected)
    restored.kill()
    _check_closed_image(image, expected)


def test_qcow2_root_squashfs_boot(uvm, num_queues):
    """Boot the real squashfs root from QCOW2 and read its full logical contents directly."""
    image = Path(uvm.fsfiles) / "rootfs.qcow2"
    _convert(uvm.rootfs_file, image)
    uvm.spawn()
    uvm.basic_config(add_root_device=False)
    uvm.add_net_iface()
    uvm.add_drive(
        "rootfs",
        image,
        is_root_device=True,
        is_read_only=True,
        image_format="Qcow2",
        num_queues=num_queues,
    )
    uvm.start()
    _assert_queue_mode(uvm, num_queues, drive_id="rootfs")
    expected = uvm.rootfs_file.read_bytes()
    _assert_guest_data(uvm, expected, device="/dev/vda")
    uvm.ssh.check_output("test -r /etc/os-release && test -d /proc/1")
    uvm.kill()
    _check_closed_image(image, expected)

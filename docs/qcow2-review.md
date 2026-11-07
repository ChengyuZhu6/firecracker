# QCOW2 correctness review and validation

## Scope and decision

Review the QCOW2 changes after `f67712627806c10a015f8662255fa34e2f732491`,
including imago `d6a40eb5c21e963131a1b439facf252480d5d57e` and Firecracker fixes
`c748dbb68` / `13bb52003`. PR
[#5741](https://github.com/firecracker-microvm/firecracker/pull/5741) is the
VMDK discussion used as the review checklist; it is not an upstream approval of
this writable QCOW2 implementation.

The implementation required correctness fixes. Image-layer and x86_64 KVM guest,
snapshot and PCI lifecycle checks now pass as recorded below. AArch64 guest
execution, supported-host-kernel coverage, representative performance and actual
power-loss testing remain explicit validation limits.

## Findings addressed

| Priority | Failure mode                                                                                                                                                                                      | Change and regression evidence                                                                                                                                                                                                                                                                                                                                                                                     |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| P1       | Refcount/L2 cache writes were ordered in userspace but could reach durable storage in the opposite order. A published mapping could refer to a zero-refcount cluster after power loss.            | imago now synchronizes dependency writes before publishing dependent metadata, including dirty cache eviction, refblock/reftable publication and old metadata reuse. The storage-event regression failed before this fix and passes afterward.                                                                                                                                                                     |
| P1       | Concurrent imago allocation could overtake an RB-cache snapshot and publish its L2 mapping in that same flush without a persisted refcount. Reopen/reallocation then overwrote the first mapping. | Re-establish L2→RB dependency after the refcount mutation, before making the mapping available. A deterministic regression reproduces a zero refcount and host-cluster alias on both ordinary and mandatory contiguous allocation paths; both fail before the fix and pass afterward. FC serializes writable access with one queue; the library still must preserve this invariant for concurrent callers.         |
| P1       | Snapshot paths could report success despite a QCOW2 flush failure, or reset other device state before learning that the block device could not be saved.                                          | Propagate QCOW2 errors, prepare block devices before destructive snapshot preparation, and cover Configuring/Inline/worker states. Guest regression retains a live vsock connection across failed snapshots and resume.                                                                                                                                                                                            |
| P1       | Writable active metadata overlaps or missing metadata refcounts could direct writes into other metadata.                                                                                          | Validate the active structural metadata at writable self-contained open; reject before mutation. This is deliberately narrower than a complete image checker.                                                                                                                                                                                                                                                      |
| P1       | An acknowledged PCI removal could unregister/drop a QCOW2 engine even when flushing failed.                                                                                                       | Preflight writable QCOW2 before teardown; preserve the device, lock and acknowledgement on failure. Retry on a subsequent external event rather than self-signalling a busy loop. The regression injects `fsync=EIO`, verifies retention, then retries. It now passes on the x86_64 KVM host. Guest hotplug/unplug also passes before and after restore with Inline/worker execution and Unsafe/Writeback caching. |
| P2       | Rejected/no-write opens could invalidate autoclear metadata such as persistent bitmaps; a cancelled header publication could complete late.                                                       | Defer autoclear publication until first mutation, serialize it and fail closed on uncertain completion. Include aligned asynchronous completion regressions.                                                                                                                                                                                                                                                       |
| P2       | Engine replacement, partial failed reads, teardown and get-id buffer handling had lifecycle or bookkeeping gaps.                                                                                  | Stage replacement before state mutation, handle same-inode PATCH without reopening its own lock, mark only copied guest bytes dirty, flush parked/dropped QCOW2 engines and accept larger get-id buffers.                                                                                                                                                                                                          |

The durability regression models one newly destaged whole write surviving a
power cut while other writes since the last successful sync are lost. It checks
ownership of active metadata/data references. It covers 512-byte clusters, 9 MiB
allocation, L1/refcount table growth, two-entry dirty caches,
discard/shrink/reuse, and failed dependency sync. It does **not** exhaustively
cover write subsets, sector tears, device firmware, all refcount invariants or
all guest-data atomicity. Process termination is a separate test, not a
substitute for this storage model or actual power-loss testing.

Dependency barriers add fsync work during some allocation/free/eviction paths.
They are needed even when guest flushes are disabled: Unsafe cache policy does
not permit publishing internally inconsistent image metadata. Performance
requires measurement on representative storage; no performance improvement is
claimed.

## Applying the community review

| Reviewer concern                                                                                                                                                                                                                   | QCOW2 handling                                                                                                                                                                                                 |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Implicit opens](https://github.com/firecracker-microvm/firecracker/pull/5741#discussion_r3776027029)                                                                                                                              | Pre-opened regular file, `DenyImplicitOpenGate` and self-contained validation. Reject backing/external-data dependencies.                                                                                      |
| [Async runtime and dependencies](https://github.com/firecracker-microvm/firecracker/pull/5741#discussion_r3311867637)                                                                                                              | imago uses `default-features=false, features=["sync"]`; the vmm dependency graph contains no tokio. Pin an audited git revision together with Cargo.lock.                                                      |
| [Effective readonly flag](https://github.com/firecracker-microvm/firecracker/pull/5741#discussion_r3311832057) and [explicit Async rejection](https://github.com/firecracker-microvm/firecracker/pull/5741#discussion_r3776128453) | Advertise actual readonly behavior; reject unsupported Async rather than silently downgrading. Reject writable multiqueue, discard and incompatible vhost-user configuration.                                  |
| [Exact dirty tracking](https://github.com/firecracker-microvm/firecracker/pull/5741#discussion_r4402606677)                                                                                                                        | Reuse a 64 KiB staging buffer and copy only completed chunks into guest memory. This avoids imago marking an entire failed read dirty.                                                                         |
| [Failure must not mutate state](https://github.com/firecracker-microvm/firecracker/pull/5741#discussion_r4402633547)                                                                                                               | Validate replacement and configuration before committing changes; retain old engine/lock on failure. Failed snapshot and unplug paths preserve retryable state.                                                |
| [Real images and nonzero contents](https://github.com/firecracker-microvm/firecracker/pull/5741#discussion_r4326875292)                                                                                                            | Generate images with qemu-img, use distinct nonzero patterns plus holes/boundaries, compare logical data and run independent qemu-img check. Hand-built metadata is reserved for targeted negative/unit tests. |
| [Ordinary files](https://github.com/firecracker-microvm/firecracker/pull/5741#discussion_r4402690283) and [worker seccomp](https://github.com/firecracker-microvm/firecracker/pull/5741#discussion_r4422666095)                    | Nonblocking open before regular-file validation; reject FIFO/device paths. Check both architectures and VMM/blk_worker filters; flock only permits nonblocking shared/exclusive acquisition.                   |
| [Snapshot encoding](https://github.com/firecracker-microvm/firecracker/pull/5741#discussion_r4425560295)                                                                                                                           | Snapshot format is version 14; serde defaults do not supply bitcode backward compatibility. QCOW2 disk contents remain separate storage state.                                                                 |

## What QEMU and Cloud Hypervisor test

[QEMU iotests](https://github.com/qemu/qemu/blob/v10.1.0/tests/qemu-iotests/README)
exercise image-layer behavior using qemu-img, qemu-io and, where necessary, QEMU
itself. They do not inherently exercise a virtio guest frontend. Useful patterns
are independent image creation, recognizable payloads, sparse and compressed
mappings, snapshots/COW, metadata corruption, injected failures, flush ordering
and consistency checking after closing the image. QEMU's
[cache implementation](https://github.com/qemu/qemu/blob/v10.1.0/block/qcow2-cache.c)
explicitly handles dependencies and storage flushes; ordering two writes alone
is insufficient.

Cloud Hypervisor at `b10a5f821c17382340bc47605c5bfec682c01483` has a separate
Rust
[QCOW2 backend](https://github.com/cloud-hypervisor/cloud-hypervisor/tree/b10a5f821c17382340bc47605c5bfec682c01483/block/src/formats/qcow)
with sync/io_uring engines, parser/refcount/cache tests and fuzz targets. Its
[guest tests](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/b10a5f821c17382340bc47605c5bfec682c01483/cloud-hypervisor/tests/integration.rs)
cover ordinary/zlib/zstd/backing images, concurrent writes and reads, fsync
storms, metadata pressure, discard and abnormal shutdown. Multiqueue cases check
consistency after terminating the VM. Those are useful test methods, not
features that this Firecracker backend automatically inherits.

| Area           | This Firecracker branch                                                                                                                                                              |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Image features | v2/v3, ordinary/deflate clusters, self-contained and unencrypted. No backing files, external data files, extended L2, zstd or discard.                                               |
| Execution      | Sync engine; one writable queue, with Inline and one-worker execution both needing tests. Readonly multiqueue uses separate engines.                                                 |
| Guest memory   | Device writes must update dirty tracking exactly for full/differential memory snapshots.                                                                                             |
| Lifecycle      | API PUT/PATCH, worker pause/reset, snapshot failure/resume, transport teardown and PCI unplug must preserve image ownership and cached mappings.                                     |
| Persistence    | Guest FLUSH requires Writeback. Snapshot and graceful teardown also flush QCOW2 metadata. Unflushed/crashed images can leak allocations; inspect a closed image with qemu-img check. |
| Ownership      | Advisory flock coordinates FC engines. QEMU's Linux OFD locking is separate; operators must exclude concurrent host tools/writers.                                                   |

## Validation evidence and remaining gates

Executed in this review session, with earlier rounds identified separately:

- Current imago: 73 sync tests, 91 async tests and four doc tests in each mode.
  Sync and async Clippy pass with the pre-existing Rust 1.99
  `chunks_exact_to_as_chunks` lint excluded; unrelated code was not rewritten.
- Current pinned backend: Linux aarch64 Firecracker block suite, 82 passed;
  drive configuration suite, 12 passed; vmm library and test code pass Clippy
  with `-D warnings`. Firecracker/seccompiler GNU debug builds pass. These debug
  builds embed the empty default seccomp policy; earlier explicit policy probes
  are separate evidence, not a filtered guest execution result.
- Final imago `d6a40eb` / QEMU 11.1.3 interoperability: 50/50 passed, with zero
  failures and a clean source checkout before/after testing. This includes 26
  sparse, six compressed, eight metadata/full-preallocated, eight
  internal-snapshot COW and two allocation/discard/shrink/reuse cases. An
  earlier fixture probe also attempted four falloc cases, rejected by QEMU on
  macOS before backend execution; these are not part of the 50 passes. There
  were 130 clean qemu-img checks, 138 matching QEMU raw comparisons, 130
  matching imago readbacks and 14 successful snapshot rollback comparisons. The
  two allocation stress cases each wrote 18,892,800 actual bytes; refcount
  tables grew from one to two/five clusters for 16-/64-bit refcounts. The heavy
  snapshot cases each overwrote 9,437,696 bytes through COW. These are
  flush/sync/close tests on macOS, separate from the fault model and Linux
  tests.
- Earlier rounds: QEMU 10.1.0 iotests 001/002/025/060/066/080/138 passed. Test
  060 used paused TCG in place of unavailable qtest; this limits equivalence to
  the official environment.
- Earlier rounds: Cloud Hypervisor's 229 `formats::qcow` tests passed with
  io_uring enabled on Linux. These were backend tests, not CLH guest boots.
- Earlier rounds: 18 QEMU interoperability cases plus internal snapshot
  rollback, bitmap handling and abrupt process exit checks passed. After the
  earlier publication-order fix the exit test reported no corruptions, with
  repairable allocation leaks. That was not a host-power-loss test.
- Earlier rounds: 12 real Firecracker preboot API cases, both seccomp policy
  architectures, and native aarch64 flock allow/deny probes passed.

### KVM validation on 2026-11-07

The explicitly authorized x86_64 KVM test host enabled the remaining runtime
checks in a scoped directory under `/work/test`. The host was an Intel Core
i7-13700, x86_64 Ubuntu 24.04.1, Linux 6.19.4-061904-generic, with Rust 1.99.1
and QEMU 8.2.2. Runtime code was built from `7fe246c13`, with imago pinned to
`d6a40eb`; this follow-up changes tests, artifact preparation and this report.

The x86_64 musl release binaries include the normal seccomp policy. Ordinary
guest cases use that policy; the four fsync-EIO cases deliberately replace it
with the fault-injection filter. The Firecracker binary SHA-256 was
`a1db99e484b2aacb79077714fd9f70ed5a0d69e5792cdce4c73882a9a9627788`.

The guest used the official CI
[Linux 5.10.271 artifact](https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/20261105-6ea6634994fa-0/x86_64/vmlinux-5.10.271)
and the existing Ubuntu 22.04 squashfs rootfs. The kernel SHA-256 was
`38d9d7a4096e95df6d236c6372e5b8f8928892503517cb723187c31cc418bc74`. Its
configuration enables `CONFIG_ACPI`, `CONFIG_PCI_MMCONFIG`, `CONFIG_HOTPLUG_PCI`
and `CONFIG_HOTPLUG_PCI_PCIE`.

The initially cached 5.10.245 image had `CONFIG_HOTPLUG_PCI` disabled and failed
the new hotplug case. Replacing only the scoped kernel copy with the CI image
made that case pass. An initial bind-mount layout also prevented hardlinks;
artifacts and jailer roots were placed within the same mount before testing.
These setup failures are excluded from the successful results.

- The complete QCOW2 guest suite: **42/42 passed**, in 220.83 seconds. It covers
  PCI/MMIO, Inline/one-worker execution, ordinary/compressed writes, live
  truncation, abrupt termination, shared readonly queues, invalid
  configurations, PATCH, root-disk boot, failed snapshot/resume, and
  full/differential restore.
- The new PCI case contributes four combinations: Inline/worker crossed with
  Unsafe/Writeback. Each performs two insert/remove cycles, one before and one
  after snapshot restore, exercising port reuse. After unflushed sparse writes,
  removal must release the file lock, stop the worker and pass independent
  qemu-img check plus a complete logical-content comparison.
- Six existing PCI snapshot/hotplug cases also passed, in 43.61 seconds:
  hotplugged-device persistence, graceful/forced removal after restore and
  pending removal completion, each with booted/restored fixture lifecycles.
  These used the official CI
  [Ubuntu 24.04 rootfs](https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/20261105-6ea6634994fa-0/x86_64/ubuntu-24.04.squashfs),
  prepared with `tools/setup-ci-artifacts.sh` to inject a scoped guest SSH key.
  The earlier cached Ubuntu 22.04 image lacks `lspci`, which these generic tests
  require. Its 42 QCOW2 results above are recorded separately.
- The KVM PCI `fsync=EIO` failure/retry unit test passed. The block suite passed
  **82 tests** after adding a close/reopen data assertion to the snapshot retry
  regression; vmm library and test code passed Clippy with `-D warnings`.

The real EIO run exposed two obsolete guest-test expectations. Dependency sync
can fail as a metadata-writeback error, so the assertion now checks the exact
QCOW2 error stage and errno 5. Also, a filter denying every fsync cannot promise
durability after termination: the observed image had two leaked clusters and
zero corruptions. This fault case allows only clean/leak-only qemu-img results,
retaining both failed-snapshot/resume and live-vsock/data checks. Positive cases
still require a clean image and matching contents. Separately, the unit test
removes the failure filter, successfully retries the flush, closes the engine,
then reopens the image and verifies the original data.

Preparing only the new rootfs exposed another fixture-tool bug: with no
`*.debug.gz` files, `xargs` still invoked gunzip on empty input and returned
123\. Adding `xargs -r` makes the real setup workflow succeed. No new dependency
or skip was introduced.

Rootfs hashes for the two test rounds:

| Image                                                          | SHA-256                                                            |
| -------------------------------------------------------------- | ------------------------------------------------------------------ |
| Cached Ubuntu 22.04 used by the 42 QCOW2 cases                 | `e9550a4ad1b979c85e6edadf2ec3bea3dd649f274f4690e2dc9347063c45c16f` |
| Official Ubuntu 24.04 download                                 | `91da33c2013bd1b018f2c1e6b3d37a3316e0ff8c19a71de640d7a4c3ca31b92b` |
| Ubuntu 24.04 after scoped SSH-key setup, used by six PCI cases | `f9567c8f1c9d9b4f2a0b5465e190a3912fb7b3c50de6c8aa62132f57a8cf3846` |

To repeat in a configured KVM development environment:

```sh
cargo test -p vmm --lib test_qcow2_unplug_retries_failed_sync
cargo test -p vmm --lib test_qcow2_snapshot_flush_error
./tools/devtool test -- integration_tests/functional/test_qcow2.py
./tools/devtool test -- integration_tests/functional/test_hotplug.py -k 'test_hotplug_preserved_after_snapshot or test_unplug_after_snapshot or test_pending_unplug_completes_after_restore'
```

The local aarch64 Linux VM still has no `/dev/kvm`, so its earlier unit/build
results are not AArch64 guest evidence. Linux 6.19 on this x86_64 host also does
not replace coverage of the project's supported host kernels. Allocation-heavy
random I/O and flush latency still need representative performance measurement.
Test VMs/images were removed between rounds; all scoped build/kernel/test
scratch is removed at completion, preserving the host's original checkout and
artifacts.

Other limits: active-open validation is not a complete refcount/COPIED-bit,
inactive snapshot or bitmap checker; same-inode PATCH does not resize; preboot
PUT replacement of an existing writable QCOW2 inode can conflict with its own
lock. Changing preboot options on that inode remains an API limitation rather
than justification for unlocking a live engine before fallible replacement.

## Commit hygiene

Six pre-existing commits in the reviewed range lack `Signed-off-by` trailers:
`40f0bbf5c`, `9149425a7`, `221c64378`, `201285399`, `3add695df`, and
`ef3684ff9`. Resolve this before upstream submission under the project's DCO
process. The new fixes carry trailers. The dependency revisions and Cargo.lock
are updated together, and each new dependency pin has validation evidence.

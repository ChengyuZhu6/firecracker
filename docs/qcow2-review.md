# QCOW2 correctness review and validation

## Scope and decision

Review the QCOW2 changes after `f67712627806c10a015f8662255fa34e2f732491`,
including imago `d6a40eb5c21e963131a1b439facf252480d5d57e` and Firecracker fixes
`c748dbb68` / `13bb52003`. PR
[#5741](https://github.com/firecracker-microvm/firecracker/pull/5741) is the
VMDK discussion used as the review checklist; it is not an upstream approval of
this writable QCOW2 implementation.

The implementation has required correctness fixes. Passing format tests alone is
insufficient for merge approval. KVM guest, snapshot and PCI lifecycle tests
remain a release gate, as do representative latency/throughput measurements.

## Findings addressed

| Priority | Failure mode                                                                                                                                                                                      | Change and regression evidence                                                                                                                                                                                                                                                                                                                                                                             |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P1       | Refcount/L2 cache writes were ordered in userspace but could reach durable storage in the opposite order. A published mapping could refer to a zero-refcount cluster after power loss.            | imago now synchronizes dependency writes before publishing dependent metadata, including dirty cache eviction, refblock/reftable publication and old metadata reuse. The storage-event regression failed before this fix and passes afterward.                                                                                                                                                             |
| P1       | Concurrent imago allocation could overtake an RB-cache snapshot and publish its L2 mapping in that same flush without a persisted refcount. Reopen/reallocation then overwrote the first mapping. | Re-establish L2→RB dependency after the refcount mutation, before making the mapping available. A deterministic regression reproduces a zero refcount and host-cluster alias on both ordinary and mandatory contiguous allocation paths; both fail before the fix and pass afterward. FC serializes writable access with one queue; the library still must preserve this invariant for concurrent callers. |
| P1       | Snapshot paths could report success despite a QCOW2 flush failure, or reset other device state before learning that the block device could not be saved.                                          | Propagate QCOW2 errors, prepare block devices before destructive snapshot preparation, and cover Configuring/Inline/worker states. Guest regression retains a live vsock connection across failed snapshots and resume.                                                                                                                                                                                    |
| P1       | Writable active metadata overlaps or missing metadata refcounts could direct writes into other metadata.                                                                                          | Validate the active structural metadata at writable self-contained open; reject before mutation. This is deliberately narrower than a complete image checker.                                                                                                                                                                                                                                              |
| P1       | An acknowledged PCI removal could unregister/drop a QCOW2 engine even when flushing failed.                                                                                                       | Preflight writable QCOW2 before teardown; preserve the device, lock and acknowledgement on failure. Retry on a subsequent external event rather than self-signalling a busy loop. The regression injects `fsync=EIO`, verifies retention, then retries. It requires KVM and has only been compiled here.                                                                                                   |
| P2       | Rejected/no-write opens could invalidate autoclear metadata such as persistent bitmaps; a cancelled header publication could complete late.                                                       | Defer autoclear publication until first mutation, serialize it and fail closed on uncertain completion. Include aligned asynchronous completion regressions.                                                                                                                                                                                                                                               |
| P2       | Engine replacement, partial failed reads, teardown and get-id buffer handling had lifecycle or bookkeeping gaps.                                                                                  | Stage replacement before state mutation, handle same-inode PATCH without reopening its own lock, mark only copied guest bytes dirty, flush parked/dropped QCOW2 engines and accept larger get-id buffers.                                                                                                                                                                                                  |

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

The local Linux VM has no `/dev/kvm`. The supplied test server was not reached
through the configured authenticated route; no guest results on that machine are
claimed. The new PCI failure/retry regression and the full QCOW2 guest suite
therefore remain unexecuted. On a KVM test machine run:

```sh
cargo test -p vmm --lib test_qcow2_unplug_retries_failed_sync
./tools/devtool test -- integration_tests/functional/test_qcow2.py
```

Run both supported CPU architectures and normal filtered worker execution, then
measure allocation-heavy random I/O and flush latency. Validate every closed
image with qemu-img check and logical-content comparison, and remove test
images/build scratch after each round. The scoped QEMU interoperability
harness/images and build outputs were removed after that round; shared checkouts
and pre-existing user files are preserved.

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

# Case Derived Concurrency

`case_derived_facts` must be treated as a single-writer dataset.

Risk:
- Concurrent writers can cause lost updates if one run overwrites another run's newer output.

Required protocol:

1. Acquire dataset-level lock.
2. Read current head/version.
3. Build next snapshot/version.
4. Atomically promote pointer to new version.
5. Release lock.

Do not:
- rely on "folder exists" checks as completion proof,
- run concurrent writers without lock + versioning.

Note:
- Daily and backfill jobs must use the same lock/pointer protocol.

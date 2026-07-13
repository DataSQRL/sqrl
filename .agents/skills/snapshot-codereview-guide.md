---
name: snapshot-codereview-guide
description: Snapshot code review guidelines
triggers:
  - /codereview
---

# SQRL Snapshot Code Review Guidelines

# Repository Code Review Guidelines

You are reviewing code for SQRL. Follow these guidelines:

## Review Decisions

### When to APPROVE
- Change is not touching snapshot files
- Change that contains snapshot updates and snapshot content update looks good based on the core logic changes 

### When to COMMENT
- A snapshot change contains updates that are undesirable
- A previously successful snapshot now contains a failure and the core logic does not indicate why
- A previously failing snapshot now contains a success and the core logic does not indicate why

## Core Principles

1. **SQRL Snapshot Files**: Any file with a `.snapshot` extension that contains the snapshot data
2. **Unit Test Snapshot Files**: Any `.txt` files under the `sqrl-testing/sqrl-testing-integration/src/test/resources/snapshots/com/datasqrl` directory

## What to Check

- **[Failing Snapshot]**: A previously succeeding snapshot now contains a failure and the core logic changes do not indicate that
- **[Succeeding Snapshot]**: A previously failing snapshot now contains a success and the core logic changes do not indicate that
- **[Snapshot Data Changes]**: A snapshot file contains updates (e.g., new data columns, changed format) that are not related to the core logic

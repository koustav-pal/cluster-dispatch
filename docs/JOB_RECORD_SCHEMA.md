# Job Record Schema Contract

This document defines the lightweight schema contract for job records under:

- `.cluster_dispatch/jobs/*.json`

These records are the canonical provenance for individual launches.

## Identity Model

Stable meaning of identity fields:

- `run_id`: deterministic run-definition identity
- `job_id`: actual scheduler/local execution identity
- `sweep_id`: parent sweep identity (sweep runs only)

Do not merge or rename these meanings.

## Required Core Fields

Consumers should assume these keys exist for new records:

- `submitted_at`
- `analysis`
- `target`
- `scheduler`
- `job_id`
- `run_id`
- `job_name`
- `state`
- `command`
- `remote_run_dir`
- `remote_log_file`
- `submission_mode`

## Common Optional Fields

Additive provenance fields that may be present:

- `command_resolved`
- `stdout`
- `stderr`
- `working_dir`
- `record_version`

Nested blocks:

- `resources`
  - `profile`, `cpus`, `memory`, `time`, `node`, `queue`, `parallel_environment`
- `paths`
  - `project_root`, `analysis_local_path`, `analysis_remote_path`, `remote_run_dir`, `remote_log_file`, `submit_script`, `working_dir`
- `sync`
  - `ignore_file`, `ignore_detected`, `ignore_used`, `source`, `destination`
- `environment`
  - `cluster_dispatch_version`, `python_version`, `hostname`
- `git` (only when project is in a git repo)
  - `repo_root`, `branch`, `commit`, `dirty`

## Sweep-Specific Fields

Present only for sweep child records:

- `sweep_id`
- `sweep_job`
- `sweep_index`
- `sweep_params`

## Backward Compatibility

- Older records may not include nested blocks (`resources`, `paths`, `sync`, `environment`, `git`).
- Older records may lack `submission_mode` or `command_resolved`.
- Readers must treat missing optional fields as unknown/absent and continue.


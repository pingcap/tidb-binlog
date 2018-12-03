

This folder contains all tests which relies on external service such as TiDB.

## Preprations

1. The flowing four executables must be copied or linked into these locations:

   - `bin/pd-server`
   - `bin/tikv-server`
   - `bin/tidb-server`

2. The following programs must be installed:

   - `mysql`(the CLI client)

3. The user executing the tests must have permission to create the folder

   `/tmp/tidb_binlog_test/pump`. All test artifacts will be written into this folder.

## Running

Run `make integration_test` to execute the integration tests. This command will

1. Check that all executables exist.
2. Build `pump` and `drainer`
3. Execute `tests/run.sh`

If the first two steps are done before, you could also run `tests/run.sh directly.

The scrip will

1. Start PD, TiKV, TiDB, Pump, Drainer in backgroud

2. Find out all `tests/*/run.sh` and run it.

   Run `tests/run.sh --debug` to pause immediately after all servers are started.

## Writing new tests

New integration tests can be written as shell script in `tests/TEST_NAME/run.sh`.

The script should exit with a nonzero error code on failure.

Serveral convenient commands are provided:

- `run_drainer`  Starts `drainer` using tests/TEST_NAME/drainer.toml (notice it may continue at the checkpoint from the last test case)
- `run_sql <SQL>` Executes an SQL query on the TiDB database(port 4000)
- `down_run_sql <SQL>` Executes an SQL query on the downstream TiDB database(port 3306)

- `check_contains <TEXT>` — Checks if the previous `run_sql` result contains the given text
  (in `-E` format)
- `check_not_contains <TEXT>` — Checks if the previous `run_sql` result does not contain the given
  text (in `-E` format)
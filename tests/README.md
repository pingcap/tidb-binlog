

This folder contains all tests which relies on external service such as TiDB.





## Running

Run `make integration_test` to execute the integration tests. This command will

1. Check `bin/drainer` executable exist.
2. Execute `tests/run.sh`



you must make sure there's external services TiDB on port 4000 and mysql or TiDB on port 3306 now.

`tests/run.sh` will run all scripts `tests/*/run.sh`



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
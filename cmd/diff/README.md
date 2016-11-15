## Diff

diff is a tool for comparing databases data.

## How to use

```
Usage of diff:
  -A	Compare all the databases. shorthand for -all-databases
  -B string
    	Compare several databases. shorthand for -databases
  -all-databases
    	Compare all the databases. This will be same as -databases with all databases selected.
  -databases string
    	Compare several databases, database names separated by commas.
  -url1 string
    	input format user[:password]@host:port (default "root@127.0.0.1:4000")
  -url2 string
    	input format user[:password]@host:port
```

## Example

```
./bin/diff -B "test,mysql" -url1 "root@127.0.0.1:3306" -url2 "root@127.0.0.1:4000"
```

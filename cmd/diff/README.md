## Diff

diff is a tool for comparing databases data.

## How to use

```
Usage of diff:
 -all-databases
    	Compare all the databases. This will be same as -databases with all databases selected.
  -A	Compare all the databases. shorthand for -all-databases
  -B string
    	Compare several databases. shorthand for -databases
  -databases string
    	Compare several databases. Note the difference in usage; in this case no tables are given. All name arguments are regarded as database names. 'USE db_name;' will be included in the output.
  -url1 string
    	user[:password]@host:port (default "root@127.0.0.1:4000")
  -url2 string
    	user:password@host:port
```


## Example

```
./bin/diff -B "test,mysql" -url1 "root@127.0.0.1:3306" -url2 "root@127.0.0.1:4000"
```

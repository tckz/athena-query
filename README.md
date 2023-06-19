athena-query
===

Execute query on Athena, then wait and output results as csv.

# Usage

```
Usage of athena-query:
  -out string
        path/to/result/file (default "/dev/stdout")
  -output-location string
        s3://bucket/key/to/out/
  -query string
        path/to/query.txt
  -stat string
        output last stat (default "/dev/stderr")
  -tsv
        tsv
  -version
        show version
  -work-group string
        name of workgroup
```

## Example

```
# q.txt
select now()

$ path/to/athena-query -query q.txt \
    -output-location s3://bucket/key/to/out/
```

# Requirements

* go 1.20

# LICENSE

See LICENCE

# HW6 Widget Request Consumer

The Python consumer polls the request S3 bucket for the lexicographically
smallest request key, reads and deletes that request, then processes it. HW6
creates widgets; update and delete request types are logged as unsupported for
this assignment.

## Setup

Install the AWS SDK dependency:

```powershell
python -m pip install -r requirements.txt
```

Configure AWS credentials using the standard AWS shared credentials file or
environment. Never put access keys in source code or command-line arguments.
The default AWS region is `us-east-1`.

## Run

Store widgets in Bucket 3:

```powershell
python consumer.py --storage s3
```

Store widgets in the DynamoDB table:

```powershell
python consumer.py --storage dynamodb
```

The defaults are request bucket `usu-cs5270-golf-requests`, widget bucket
`usu-cs5270-golf-web`, and table `widgets`. Override them with
`--request-bucket`, `--widget-bucket`, `--table-name`, `--region`, or
`--profile`. Use `--max-requests N` for a bounded run. Logs are written to
`consumer.log` by default; change the location with `--log-file`.

## Tests

Run the unit tests without AWS access:

```powershell
python -m unittest discover -s tests -v
```

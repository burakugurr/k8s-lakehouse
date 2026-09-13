# LocalStack Bucket Credentials

This file stores the separate S3 credentials for the two LocalStack buckets used by the lakehouse stack.

## Bucket: warehouse

- Bucket name: `warehouse`
- Access key: `LKIAQAAAAAAANY4X75TF`
- Secret key: `dwQj/4RRz5kaIE8hmIAIBLlIppRGBO+HXIhgIRnv`
- Region: `us-east-1`
- Endpoint: `http://localstack-service.bigdata.svc.cluster.local:4566`

## Bucket: bigdata

- Bucket name: `bigdata`
- Access key: `LKIAQAAAAAAACQFDG4AH`
- Secret key: `7ZaSf8qGHQhfHzg3ssuykNQhJktMOEhCpSNoouEA`
- Region: `us-east-1`
- Endpoint: `http://localstack-service.bigdata.svc.cluster.local:4566`

## STS identity

- Caller identity from LocalStack STS:
  - Account: `000000000000`
  - ARN: `arn:aws:iam::000000000000:root`

## Notes

- These are LocalStack emulated AWS credentials, not real AWS credentials.
- They are suitable for local development and testing only.
- Use the bucket-specific access key/secret pair when writing to the appropriate bucket.

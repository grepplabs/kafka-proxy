# AWS MSK IAM Authentication Plugin

This plugin provides AWS MSK IAM authentication for the Kafka Proxy using SASL/OAUTHBEARER mechanism. It automatically handles token refresh to ensure authentication doesn't expire.

## Features

- **Multiple Authentication Methods**: Supports default credentials, named profiles, and IAM role assumption
- **Automatic Token Refresh**: Tokens are automatically refreshed 5 minutes before expiry
- **Role Assumption**: Supports assuming IAM roles with optional external ID
- **Error Handling**: Robust error handling with exponential backoff for token generation

## Configuration Options

| Option | Required | Description | Default |
|--------|----------|-------------|---------|
| `--region` | Yes | AWS region where MSK cluster is located | - |
| `--profile` | No | AWS named profile to use for authentication | - |
| `--role-arn` | No | IAM role ARN to assume | - |
| `--external-id` | No | External ID for role assumption | - |
| `--session-name` | No | Session name for role assumption | `kafka-proxy` |
| `--timeout` | No | Request timeout in seconds | `10` |

## Authentication Methods

### 1. Default Credentials
Uses the default AWS credential provider chain (environment variables, IAM roles, etc.).

```bash
./plugin-aws-msk-iam-provider --region us-east-1
```

### 2. Named Profile
Uses a specific AWS named profile.

```bash
./plugin-aws-msk-iam-provider --region us-east-1 --profile my-profile
```

### 3. IAM Role Assumption
Assumes an IAM role for authentication.

```bash
./plugin-aws-msk-iam-provider --region us-east-1 --role-arn arn:aws:iam::123456789012:role/MyRole --session-name kafka-proxy
```

### 4. IAM Role with External ID
Assumes an IAM role with external ID for enhanced security.

```bash
./plugin-aws-msk-iam-provider --region us-east-1 --role-arn arn:aws:iam::123456789012:role/MyRole --external-id my-external-id --session-name kafka-proxy
```

## Integration with Kafka Proxy

To use this plugin with the Kafka Proxy, configure it in your proxy configuration:

```yaml
token-provider:
  plugin: aws-msk-iam-provider
  args:
    - "--region=us-east-1"
    - "--profile=my-profile"
    # or for role assumption:
    # - "--role-arn=arn:aws:iam::123456789012:role/MyRole"
    # - "--session-name=kafka-proxy"
```

## Token Refresh

The plugin automatically refreshes tokens 5 minutes before they expire. AWS MSK IAM tokens are typically valid for 15 minutes, and the plugin sets an internal expiry of 14 minutes to ensure safe refresh timing.

## Error Handling

- **Initial Token Generation**: Uses exponential backoff with up to 3 retries
- **Token Refresh**: Uses exponential backoff with up to 60 seconds max elapsed time
- **Graceful Degradation**: Continues operation even if refresh fails, will retry on next request

## Dependencies

This plugin uses the [AWS MSK IAM SASL Signer for Go](https://github.com/aws/aws-msk-iam-sasl-signer-go) library to generate authentication tokens.

## Troubleshooting

### Common Issues

1. **Access Denied**: Ensure the IAM user/role has the necessary permissions for MSK cluster access
2. **Region Mismatch**: Verify the region matches your MSK cluster location
3. **Profile Not Found**: Check that the AWS profile exists in your credentials file
4. **Role Assumption Failed**: Verify the role ARN and ensure your credentials can assume the role

### Debug Mode

To enable debug logging for AWS credentials, set the environment variable:

```bash
export AWS_DEBUG_CREDS=true
```

This will log the IAM identity being used for authentication.

## Security Considerations

- **External ID**: Use external ID when assuming roles for enhanced security
- **Session Names**: Use descriptive session names for better audit trails
- **Credentials**: Ensure credentials are properly secured and rotated regularly
- **Network**: Use TLS encryption for all communications with MSK clusters 

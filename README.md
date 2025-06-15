# mmdbcli

## Install

```bash
go install github.com/nonhumantrades/mmdb-client/cmd/mmdbcli@latest
```

## Configuration

```bash
mmdbcli s3 set --bucket "my-bucket" --url "https://my-bucket.s3.amazonaws.com" --access-key "my-access-key" --secret-key "my-secret-key" --region "us-east-1"
```

## Usage

### List tables

```bash
mmdbcli list-tables
```

### View S3 configuration

```bash
mmdbcli s3 view
```

### Backup

Full backup:
```bash
mmdbcli backup full
```

Incremental backup:
```bash
mmdbcli backup inc --full-key "full-1289.mmdb"
```

### Restore

Full restore:
```bash
mmdbcli restore --key "full-1289.mmdb"
```

Incremental restore:
```bash
mmdbcli restore --key "inc-1289-1876.mmdb"
```

### Sync

Sync a table:
```bash
mmdbcli sync --table "orderbook"
```
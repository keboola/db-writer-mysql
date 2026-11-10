# MySQL DB Writer

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/keboola/db-writer-mysql/blob/master/LICENSE.md)

Imports CSV data into MySQL Database.

## Example configuration

```json
    {
      "db": {        
        "host": "HOST",
        "port": "PORT",
        "database": "DATABASE",
        "user": "USERNAME",
        "#password": "PASSWORD",
        "ssh": {
          "enabled": true,
          "keys": {
            "#private": "ENCRYPTED_PRIVATE_SSH_KEY",
            "public": "PUBLIC_SSH_KEY"
          },
          "sshHost": "PROXY_HOSTNAME"
        }
      },
      "tableId": "simple",
      "dbName": "dbo.simple",
      "export": true, 
      "incremental": true,
      "primaryKey": ["id"],
      "items": [
         {
           "name": "id",
           "dbName": "id",
           "type": "int",
           "size": null,
           "nullable": null,
           "default": null
         },
         {
           "name": "name",
           "dbName": "name",
           "type": "nvarchar",
           "size": 255,
           "nullable": null,
           "default": null
         },
         {
           "name": "glasses",
           "dbName": "glasses",
           "type": "nvarchar",
           "size": 255,
           "nullable": null,
           "default": null
         }
      ]
    }
```

## Descriptions as comments

`propagateDescriptions` (default `true`) writes the Storage table and column descriptions as MySQL comments
after the data is loaded. Set it to `false` to turn it off.

- Only non-empty descriptions are written. An existing comment is never cleared.
- The table comment is set with `ALTER TABLE ... COMMENT`. Column comments are set with `MODIFY COLUMN`
  on the existing table, restating the current column definition. Generated columns and all spatial columns
  are skipped with a warning.
- Comments that already match are skipped, so an unchanged table gets no `ALTER TABLE`.
- `ALTER TABLE` waits for a metadata lock for at most 10 seconds and is not retried. If another session holds
  the table, the remaining comments are not written and the job logs a warning instead of waiting.
- Comments longer than the MySQL limit (2048 characters for tables, 1024 for columns) are truncated, and
  4-byte characters such as emoji are replaced with `U+FFFD`. Both produce a warning.

## Development

1. Install dependencies

        docker-compose run --rm dev composer install

2. Run tests:

        docker-compose run --rm tests

## License

MIT licensed, see [LICENSE](./LICENSE) file.

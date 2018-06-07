# MySQL DB Writer

[![Docker Repository on Quay](https://quay.io/repository/keboola/db-writer-mysql/status "Docker Repository on Quay")](https://quay.io/repository/keboola/db-writer-mysql)
[![Build Status](https://travis-ci.org/keboola/db-writer-mysql.svg?branch=master)](https://travis-ci.org/keboola/db-writer-mysql)
[![Code Climate](https://codeclimate.com/github/keboola/db-writer-mysql/badges/gpa.svg)](https://codeclimate.com/github/keboola/db-writer-mysql)
[![Test Coverage](https://codeclimate.com/github/keboola/db-writer-mysql/badges/coverage.svg)](https://codeclimate.com/github/keboola/db-writer-mysql/coverage)
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
        "password": "PASSWORD",
        "ssh": {
          "enabled": true,
          "keys": {
            "private": "ENCRYPTED_PRIVATE_SSH_KEY",
            "public": "PUBLIC_SSH_KEY"
          },
          "sshHost": "PROXY_HOSTNAME"
        }
      },
      "tables": [
        {
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
      ]
    }
```

## Development

1. Install dependencies

        docker-composer run --rm dev composer install

2. Generate SSH key pair for SSH proxy:

        source ./vendor/keboola/db-writer-common/tests/generate-ssh-keys.sh
    
3. Run tests:

        docker-compose run --rm tests

#!/bin/bash

if [[ "$MYSQL_VERSION" =~ ^5\.7 ]]; then
  exec docker-entrypoint.sh mysqld --local-infile --port=3306 --default-authentication-plugin=mysql_native_password
else
  exec docker-entrypoint.sh mysqld --local-infile --port=3306 --mysql-native-password=ON
fi
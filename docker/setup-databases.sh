#!/bin/bash

# This script generates standard SQL for creating a basic user:password@database
echo "Creating user users and databases ..."

DB_CONFIG=$(cat /usr/local/bin/user-databases.txt)
DB_INIT=/docker-entrypoint-initdb.d/create-user-databases.sql

for USER_DATABASE_INFO in $DB_CONFIG; do
  USER=$(echo $USER_DATABASE_INFO | cut -d "@" -f 1 | cut -d ":" -f 1)
  PASSWORD=$(echo $USER_DATABASE_INFO | cut -d "@" -f 1 | cut -d ":" -f 2)
  DB=$(echo $USER_DATABASE_INFO | cut -d "@" -f 2)
  echo "\connect postgres;" >> $DB_INIT
  echo "CREATE DATABASE $DB;" >> $DB_INIT
  echo "REVOKE CONNECT ON DATABASE $DB FROM PUBLIC;" >> $DB_INIT
  echo "CREATE USER $USER PASSWORD '$PASSWORD';" >> $DB_INIT
  echo "GRANT CONNECT ON DATABASE $DB TO $USER;" >> $DB_INIT
  echo "GRANT ALL ON DATABASE $DB TO $USER WITH GRANT OPTION;" >> $DB_INIT
  echo "\connect $DB;" >> $DB_INIT
  echo "CREATE SCHEMA $USER AUTHORIZATION $USER;" >> $DB_INIT
  if [ -f "/usr/local/bin/custom/$DB.sql" ]; then
    echo "Adding custom SQL from $DB.sql"
    cat "/usr/local/bin/custom/$DB.sql" >> $DB_INIT
  fi;
done
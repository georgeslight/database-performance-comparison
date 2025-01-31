CREATE TABLE qdaba.<table_name>_pg ENGINE = PostgreSQL('<host>:<port>', '<database>', '<table_name>', '<user>', '<password>', '<schema>');

CREATE TABLE <table_name> AS <table_name>_pg;

INSERT INTO <table_name> SELECT * FROM <table_name>_pg;

clickhouse-client --query="INSERT INTO <table_name> SELECT * FROM <table_name>_pg;"

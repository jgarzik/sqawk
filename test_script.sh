#!/bin/bash
# Start sqawk in interactive mode with a test file
echo "CREATE TABLE test_table (id INT, name TEXT, value FLOAT) LOCATION './test_output.csv' STORED AS TEXTFILE WITH (DELIMITER=',');" | cargo run -- tests/data/sample.csv --interactive

#!/bin/bash

# Create a file with input commands for the REPL
cat > test_input.txt << 'END'
CREATE TABLE test_types (id INTEGER, name TEXT, price FLOAT, active BOOLEAN) LOCATION './test_types.csv' STORED AS TEXTFILE WITH (DELIMITER=',');
.schema test_types
.exit
END

# Run sqawk in interactive mode with these commands
cat test_input.txt | cargo run -- tests/data/sample.csv --interactive

# Temporary Test Files Directory

This directory contains temporary files used for testing purposes. Any test-generated data or test input files should be placed here rather than in the root directory of the codebase.

The current files in this directory:

- `test_changes_command.txt` - Test data for the `.changes` REPL command
- `test_exit_code.txt` - Test data for the exit code feature of the `.exit` command
- `test_pattern_matching.txt` - Test data for pattern matching in the `.tables` command
- `test_repl_commands.txt` - Combined test data for multiple REPL commands

When adding new test files, please follow the naming convention of prefixing with `test_` and adding a descriptive name.
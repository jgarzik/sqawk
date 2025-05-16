use sqawk::csv_handler::CsvHandler;
use sqawk::error::SqawkError;

#[test]
fn test_csv_with_comments() {
    // Create the CSV handler
    let csv_handler = CsvHandler::new();
    
    // Load the commented CSV file
    let table = csv_handler.load_csv("tests/data/commented.csv", None, None).unwrap();
    
    // Verify the correct data was loaded (comments should be ignored)
    assert_eq!(table.rows().len(), 3);
    assert_eq!(table.name(), "commented");
    
    // Check specific row data
    let rows = table.rows();
    assert_eq!(rows[0][1].to_string(), "Alice");
    assert_eq!(rows[1][1].to_string(), "Bob");
    assert_eq!(rows[2][1].to_string(), "Charlie");
}

#[test]
fn test_malformed_csv_with_error_recovery() {
    // Create the CSV handler
    let csv_handler = CsvHandler::new();
    
    // Try to load the malformed CSV file with error recovery enabled
    let table = csv_handler.load_csv("tests/data/malformed.csv", None, Some(true)).unwrap();
    
    // With flexible mode, all rows except the one with unclosed quote should be loaded
    // Rows with too many/few fields should be handled
    assert_eq!(table.rows().len(), 4);
    
    // Check specific row data
    let rows = table.rows();
    assert_eq!(rows[0][1].to_string(), "Alice");
    assert_eq!(rows[1][1].to_string(), "Charlie");
    // Row with missing field should have empty/null third field
    assert_eq!(rows[2][0].to_string(), "4");
    assert_eq!(rows[2][1].to_string(), "Missing");
    // Row with extra fields should have been loaded with first 3 fields
    assert_eq!(rows[3][0].to_string(), "5");
    assert_eq!(rows[3][1].to_string(), "Too");
}

#[test]
fn test_malformed_csv_without_recovery() {
    // Create the CSV handler
    let csv_handler = CsvHandler::new();
    
    // Try to load the malformed CSV file without error recovery
    let result = csv_handler.load_csv("tests/data/malformed.csv", None, None);
    
    // Verify that we get an error
    assert!(result.is_err());
    
    // Check that the error contains file path and line information
    match result.unwrap_err() {
        SqawkError::CsvParseError { file, line, error } => {
            assert!(file.contains("malformed.csv"));
            // The error should be on line 3 (the unclosed quote)
            assert_eq!(line, 3);
            assert!(error.contains("quote"));
        }
        err => panic!("Expected CsvParseError, got {:?}", err),
    }
}

#[test]
fn test_csv_with_custom_columns() {
    // Create the CSV handler
    let csv_handler = CsvHandler::new();
    
    // Define custom column names
    let custom_columns = Some(vec![
        "user_id".to_string(),
        "full_name".to_string(),
        "user_age".to_string(),
    ]);
    
    // Load the commented CSV file with custom columns
    let table = csv_handler.load_csv("tests/data/commented.csv", custom_columns, None).unwrap();
    
    // Verify the column names were properly set
    let columns = table.columns();
    assert_eq!(columns[0], "user_id");
    assert_eq!(columns[1], "full_name");
    assert_eq!(columns[2], "user_age");
    
    // Verify the data was loaded correctly
    assert_eq!(table.rows().len(), 3);
}
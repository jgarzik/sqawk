use std::path::PathBuf;
use std::error::Error;

use sqawk::table::{Table, Value};
use sqawk::csv_handler::CsvHandler;
use sqawk::sql_executor::SqlExecutor;

#[test]
fn test_filtered_select_manual() -> Result<(), Box<dyn Error>> {
    // Create a simple in-memory table
    let mut columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    let mut table = Table::new("people", columns, None);
    
    // Add some rows
    table.add_row(vec![
        Value::Integer(1),
        Value::String("Alice".to_string()),
        Value::Integer(30),
    ])?;
    
    table.add_row(vec![
        Value::Integer(2),
        Value::String("Bob".to_string()),
        Value::Integer(25),
    ])?;
    
    table.add_row(vec![
        Value::Integer(3),
        Value::String("Charlie".to_string()),
        Value::Integer(35),
    ])?;
    
    // Create a CSV handler and add our table
    let mut csv_handler = CsvHandler::new();
    csv_handler.add_table(table)?;
    
    // Create an SQL executor
    let mut executor = SqlExecutor::new(csv_handler);
    
    // Execute a filtered SELECT query
    let result = executor.execute("SELECT name FROM people WHERE age = 30")?;
    
    // Check that the result contains only Alice
    if let Some(result_table) = result {
        assert_eq!(result_table.row_count(), 1);
        let row = result_table.rows()[0].clone();
        assert_eq!(row[0], Value::String("Alice".to_string()));
    } else {
        panic!("No result table returned");
    }
    
    Ok(())
}
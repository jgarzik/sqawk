use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
    let sql = "CREATE TABLE test_table (id INT, name TEXT) LOCATION './test_output.csv' STORED AS TEXTFILE WITH (DELIMITER=',');";
    println!("Testing SQL: {}", sql);
    
    let dialect = GenericDialect {}; // First try with generic dialect
    
    match Parser::parse_sql(&dialect, sql) {
        Ok(ast) => {
            println!("Successfully parsed with GenericDialect!");
            println!("AST: {:#?}", ast);
        },
        Err(e) => {
            println!("Failed to parse with GenericDialect: {}", e);
        }
    }
    
    // Also try with HiveDialect which is known to support LOCATION
    use sqlparser::dialect::HiveDialect;
    let hive_dialect = HiveDialect {};
    
    match Parser::parse_sql(&hive_dialect, sql) {
        Ok(ast) => {
            println!("Successfully parsed with HiveDialect!");
            println!("AST: {:#?}", ast);
        },
        Err(e) => {
            println!("Failed to parse with HiveDialect: {}", e);
        }
    }
}

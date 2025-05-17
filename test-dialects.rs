// Test different SQL dialects for CREATE TABLE with LOCATION
extern crate sqlparser;

use sqlparser::dialect::{GenericDialect, HiveDialect, SnowflakeDialect};
use sqlparser::parser::Parser;

fn main() {
    let sql = "CREATE TABLE test_table (id INT, name TEXT) LOCATION './test_output.csv' STORED AS TEXTFILE WITH (DELIMITER=',');";
    println!("Testing CREATE TABLE with LOCATION:");
    println!("{}\n", sql);
    
    // Try different dialects
    let dialects = [
        ("Generic", GenericDialect {}),
        ("Hive", HiveDialect {}),
        ("Snowflake", SnowflakeDialect {})
    ];
    
    for (name, dialect) in dialects.iter() {
        println!("=== {} Dialect ===", name);
        match Parser::parse_sql(dialect, sql) {
            Ok(statements) => {
                println!("  Parsed successfully!");
                if let Some(stmt) = statements.first() {
                    println!("  Statement type: {:?}", std::mem::discriminant(stmt));
                    
                    use sqlparser::ast::Statement;
                    if let Statement::CreateTable { location, .. } = stmt {
                        println!("  LOCATION clause: {:?}", location);
                    } else {
                        println!("  Not a CREATE TABLE statement!");
                    }
                }
            },
            Err(e) => println!("  Parse error: {}", e)
        }
        println!();
    }
}

// Test program to debug how different SQL dialects handle CREATE TABLE with LOCATION
use sqlparser::dialect::{GenericDialect, HiveDialect, PostgreSqlDialect, MySqlDialect, SnowflakeDialect, RedshiftDialect};
use sqlparser::parser::Parser;

fn main() {
    // Test SQL with LOCATION clause
    let sql = "CREATE TABLE test_table (id INT, name TEXT) LOCATION './test_output.csv' STORED AS TEXTFILE WITH (DELIMITER=',');";
    
    println!("Testing SQL CREATE TABLE with LOCATION clause:");
    println!("{}\n", sql);
    
    // Test with various dialects to see which one handles LOCATION properly
    let dialects = [
        ("Hive", HiveDialect {}),
        ("GenericDialect", GenericDialect {}),
        ("Snowflake", SnowflakeDialect {}), 
        ("PostgreSQL", PostgreSqlDialect {}),
        ("MySQL", MySqlDialect {}),
        ("Redshift", RedshiftDialect {})
    ];
    
    for (name, dialect) in dialects.iter() {
        println!("Testing with {} dialect:", name);
        match Parser::parse_sql(dialect, sql) {
            Ok(ast) => {
                println!("  ✓ SQL parsed successfully");
                // Check if the first statement is a CREATE TABLE and if it has a LOCATION
                if let Some(stmt) = ast.first() {
                    if let sqlparser::ast::Statement::CreateTable { location, .. } = stmt {
                        match location {
                            Some(loc) => println!("  ✓ LOCATION clause parsed: '{}'", loc),
                            None => println!("  ✗ LOCATION clause NOT parsed (came back as None)")
                        }
                    } else {
                        println!("  ✗ Statement not recognized as CREATE TABLE");
                    }
                }
            },
            Err(e) => println!("  ✗ Parser error: {}", e)
        }
        println!();
    }
}

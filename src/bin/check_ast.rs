use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
    let dialect = GenericDialect {};
    let sql = "DELETE FROM mytable WHERE id > 5";
    let result = Parser::parse_sql(&dialect, sql);
    
    match result {
        Ok(statements) => {
            for (i, stmt) in statements.iter().enumerate() {
                println!("Statement {}: {:#?}", i + 1, stmt);
            }
        },
        Err(e) => {
            println!("Error parsing SQL: {}", e);
        }
    }
}
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
    let sql = "CREATE TABLE test_table (id INT, name TEXT) LOCATION './test_output.csv' STORED AS TEXTFILE WITH (DELIMITER=',');";
    println!("Testing SQL: {}", sql);

    let dialect = GenericDialect {};
    match Parser::parse_sql(&dialect, sql) {
        Ok(ast) => {
            println!("Parsed AST: {:#?}", ast);
        },
        Err(e) => {
            println!("Error parsing SQL: {}", e);
        }
    }
}

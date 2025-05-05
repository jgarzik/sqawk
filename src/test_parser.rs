use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
    let dialect = GenericDialect {};
    let sql = "SELECT COUNT(*), SUM(age), AVG(salary), MIN(price), MAX(value) FROM users";
    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    println!("AST: {:#?}", ast);
}
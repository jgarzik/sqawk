use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
    let dialect = GenericDialect {};
    let sql = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id";
    
    let statements = Parser::parse_sql(&dialect, sql).unwrap();
    
    println!("{:#?}", statements);
}
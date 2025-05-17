fn main() {
    let sql = "CREATE TABLE test_table (id INT, name TEXT) LOCATION './test_output.csv' STORED AS TEXTFILE WITH (DELIMITER=',');";
    println!("We want to test the SQL: {}", sql);
    println!("But need sqlparser to do so!");
}

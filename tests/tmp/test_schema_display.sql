CREATE TABLE typed_table (
  id INTEGER,
  name TEXT,
  price FLOAT,
  active BOOLEAN
) LOCATION './typed_table.csv' STORED AS TEXTFILE WITH (DELIMITER=',');

INSERT INTO typed_table VALUES
  (1, 'Widget', 19.99, true),
  (2, 'Gadget', 24.95, false),
  (3, 'Doohickey', 14.50, true);

//! `tsq gen`: realistic multi-table data with foreign keys, optionally
//! salted with edge-case cells, in any of the layouts sqawk reads.
//!
//! Tables (N = `--rows`):
//!
//! | table       | rows              | keys                                     |
//! |-------------|-------------------|------------------------------------------|
//! | customers   | N                 | customer_id; referred_by -> customers    |
//! | products    | max(N/100, min(N, 100)) | product_id                         |
//! | orders      | 3N                | order_id; customer_id -> customers       |
//! | order_items | 10N               | item_id; order_id, product_id            |
//! | reviews     | N/2 (at least 1)  | review_id; customer_id, product_id       |
//!
//! Distributions: 85% of customers active; credit scores uniform 300-850;
//! 20% of customers have notes and 30% a referrer; 5% of products
//! discontinued and 90% described; 80% of orders go to the first 20% of
//! customers (Pareto); half of the orders carry notes; 85% of reviews have a
//! body. Money is whole cents written in sqawk's canonical real spelling
//! (`12.5`, `7.0`), so it loads as REAL.
//!
//! `--edge-rate F` then replaces each non-key cell, with probability F, by
//! an edge value (see `EDGE_VALUES`). Key columns stay clean so the foreign
//! keys and the window-function tie breakers the differential tester relies
//! on stay unique. The base data does not depend on the edge rate: the
//! injector draws from its own stream.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use crate::data::{write_table, Format, Table};
use crate::rng::Rng;

const FIRST_NAMES: &[&str] = &[
    "James",
    "Mary",
    "John",
    "Patricia",
    "Robert",
    "Jennifer",
    "Michael",
    "Linda",
    "William",
    "Elizabeth",
    "David",
    "Barbara",
    "Richard",
    "Susan",
    "Joseph",
    "Jessica",
    "Thomas",
    "Sarah",
    "Charles",
    "Karen",
    "Christopher",
    "Nancy",
    "Daniel",
    "Lisa",
    "Matthew",
    "Betty",
    "Anthony",
    "Margaret",
    "Mark",
    "Sandra",
    "Donald",
    "Ashley",
    "Steven",
    "Kimberly",
    "Paul",
    "Emily",
    "Andrew",
    "Donna",
    "Joshua",
    "Michelle",
    "Kenneth",
    "Dorothy",
    "Kevin",
    "Carol",
    "Brian",
    "Amanda",
    "George",
    "Melissa",
    "Timothy",
    "Deborah",
    "Ronald",
    "Stephanie",
    "Edward",
    "Rebecca",
    "Jason",
    "Sharon",
    "Jeffrey",
    "Laura",
    "Ryan",
    "Cynthia",
    "Jacob",
    "Kathleen",
    "Gary",
    "Amy",
];

const LAST_NAMES: &[&str] = &[
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson",
    "Walker",
    "Young",
    "Allen",
    "King",
    "Wright",
    "Scott",
    "Torres",
    "Nguyen",
    "Hill",
    "Flores",
    "Green",
    "Adams",
    "Nelson",
    "Baker",
    "Hall",
    "Rivera",
    "Campbell",
    "Mitchell",
    "Carter",
    "Roberts",
    "Gomez",
    "Phillips",
    "Evans",
    "Turner",
    "Diaz",
    "Parker",
];

const CITIES: &[(&str, &str)] = &[
    ("New York", "NY"),
    ("Los Angeles", "CA"),
    ("Chicago", "IL"),
    ("Houston", "TX"),
    ("Phoenix", "AZ"),
    ("Philadelphia", "PA"),
    ("San Antonio", "TX"),
    ("San Diego", "CA"),
    ("Dallas", "TX"),
    ("San Jose", "CA"),
    ("Austin", "TX"),
    ("Jacksonville", "FL"),
    ("Fort Worth", "TX"),
    ("Columbus", "OH"),
    ("Charlotte", "NC"),
    ("San Francisco", "CA"),
    ("Indianapolis", "IN"),
    ("Seattle", "WA"),
    ("Denver", "CO"),
    ("Boston", "MA"),
];

const COUNTRIES: &[&str] = &["USA", "Canada", "Mexico", "UK", "Germany"];

const SUBCATEGORIES: &[(&str, &[&str])] = &[
    (
        "Electronics",
        &["Phones", "Laptops", "Tablets", "Cameras", "Audio"],
    ),
    (
        "Clothing",
        &["Shirts", "Pants", "Dresses", "Shoes", "Accessories"],
    ),
    (
        "Home & Garden",
        &["Furniture", "Kitchen", "Bedding", "Decor", "Tools"],
    ),
    (
        "Sports",
        &[
            "Fitness",
            "Outdoor",
            "Team Sports",
            "Water Sports",
            "Winter",
        ],
    ),
    (
        "Books",
        &["Fiction", "Non-Fiction", "Science", "History", "Children"],
    ),
    (
        "Toys",
        &[
            "Action Figures",
            "Board Games",
            "Puzzles",
            "Dolls",
            "Building",
        ],
    ),
    (
        "Automotive",
        &["Parts", "Accessories", "Tools", "Care", "Electronics"],
    ),
    (
        "Health",
        &[
            "Vitamins",
            "First Aid",
            "Personal Care",
            "Fitness",
            "Medical",
        ],
    ),
    (
        "Beauty",
        &["Skincare", "Makeup", "Hair Care", "Fragrance", "Bath"],
    ),
    (
        "Food",
        &["Snacks", "Beverages", "Organic", "Frozen", "Pantry"],
    ),
];

const ORDER_STATUSES: &[&str] = &["pending", "shipped", "delivered", "cancelled", "returned"];
const SHIPPING_METHODS: &[&str] = &["standard", "express", "overnight", "pickup"];
const EMAIL_DOMAINS: &[&str] = &[
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "example.com",
    "mail.com",
    "proton.me",
    "icloud.com",
];
const PRODUCT_ADJECTIVES: &[&str] = &[
    "Premium",
    "Pro",
    "Ultra",
    "Basic",
    "Advanced",
    "Classic",
    "Modern",
    "Compact",
    "Deluxe",
    "Essential",
    "Elite",
    "Standard",
    "Professional",
    "Portable",
    "Smart",
];
const PRODUCT_NOUNS: &[&str] = &[
    "Widget",
    "Gadget",
    "Device",
    "Tool",
    "Kit",
    "Set",
    "Pack",
    "Bundle",
    "System",
    "Unit",
    "Module",
    "Component",
    "Accessory",
    "Item",
    "Product",
];
const WORDS: &[&str] = &[
    "lorem",
    "ipsum",
    "dolor",
    "sit",
    "amet",
    "consectetur",
    "adipiscing",
    "elit",
    "sed",
    "do",
    "eiusmod",
    "tempor",
    "incididunt",
    "ut",
    "labore",
];
const SPECIAL_WORDS: &[&str] = &["can't", "won't", "it's", "O'Brien", "Smith & Co"];

/// Cell values that break naive readers, type inference and comparisons.
pub const EDGE_VALUES: &[&str] = &[
    // NULL (an empty field); the injector also writes some as a quoted ""
    "",
    // integer extremes and the first values past them (TEXT to sqawk)
    "9223372036854775807",
    "-9223372036854775808",
    "9223372036854775808",
    "-9223372036854775809",
    "18446744073709551616",
    // numeric-looking text that is not canonical, so it loads as TEXT
    "007",
    "1.50",
    "1e3",
    " 5",
    "5 ",
    "+5",
    "-0",
    "-0.0",
    ".5",
    "5.",
    "0.30000000000000004",
    "0x10",
    "12abc",
    "1,234",
    // canonical reals at the edges of the rendering rules
    "1.0e+20",
    "1.0e-05",
    "0.0001",
    "123456789012345.0",
    // words that are values in other systems
    "NaN",
    "inf",
    "-Inf",
    "true",
    "false",
    "NULL",
    "null",
    // Unicode: accents, combining marks, emoji, right-to-left, case mapping
    "héllo",
    "e\u{301}",
    "🎉",
    "שלום",
    "\u{200f}x",
    "ß",
    "İ",
    "ﬁ",
    "\u{a0}nbsp",
    // record and field separators inside a field
    "a\nb",
    "a\r\nb",
    "a\rb",
    "say \"hi\"",
    "a,b",
    "a\tb",
    "  pad  ",
    // dates that are not dates
    "2024-02-30",
    "2024-13-01",
];

/// `count` cents as sqawk's canonical real text: `12.5`, `7.0`, `0.05`.
fn cents_text(count: i64) -> String {
    let (whole, frac) = (count / 100, count % 100);
    if frac == 0 {
        format!("{whole}.0")
    } else if frac % 10 == 0 {
        format!("{whole}.{}", frac / 10)
    } else {
        format!("{whole}.{frac:02}")
    }
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn random_date(r: &mut Rng, from_year: i64, to_year: i64) -> String {
    let year = r.range(from_year, to_year);
    let month = r.range(1, 12);
    let days = match month {
        2 if is_leap(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    let day = r.range(1, days);
    format!("{year:04}-{month:02}-{day:02}")
}

fn random_text(r: &mut Rng, special: bool) -> String {
    let n = r.range(3, 10) as usize;
    let mut words: Vec<&str> = (0..n).map(|_| *r.pick(WORDS)).collect();
    if special && r.chance(0.1) {
        words.push(*r.pick(SPECIAL_WORDS));
    }
    words.join(" ")
}

/// Row counts for a base of `rows` customers.
pub struct Sizes {
    pub customers: usize,
    pub products: usize,
    pub orders: usize,
    pub order_items: usize,
    pub reviews: usize,
}

impl Sizes {
    pub fn new(rows: usize) -> Self {
        let rows = rows.max(1);
        Sizes {
            customers: rows,
            products: (rows / 100).max(rows.min(100)),
            orders: rows * 3,
            order_items: rows * 10,
            reviews: (rows / 2).max(1),
        }
    }
}

/// The five tables for `seed`, without edge values.
pub fn base_tables(seed: u64, rows: usize) -> Vec<Table> {
    let s = Sizes::new(rows);
    let mut r = Rng::stream(seed, 0);

    let mut customers = Table::new(
        "customers",
        &[
            "customer_id",
            "name",
            "email",
            "city",
            "state",
            "country",
            "signup_date",
            "is_active",
            "credit_score",
            "referred_by",
            "notes",
        ],
    );
    for i in 1..=s.customers {
        let first = *r.pick(FIRST_NAMES);
        let last = *r.pick(LAST_NAMES);
        let domain = *r.pick(EMAIL_DOMAINS);
        let (city, state) = *r.pick(CITIES);
        let referred_by = if i > 1 && r.chance(0.3) {
            r.range(1, i as i64 - 1).to_string()
        } else {
            String::new()
        };
        customers.rows.push(vec![
            i.to_string(),
            format!("{first} {last}"),
            format!(
                "{}.{}{}@{}",
                first.to_lowercase(),
                last.to_lowercase(),
                i % 1000,
                domain
            ),
            city.to_string(),
            state.to_string(),
            r.pick(COUNTRIES).to_string(),
            random_date(&mut r, 2020, 2025),
            (r.chance(0.85) as i32).to_string(),
            r.range(300, 850).to_string(),
            referred_by,
            if r.chance(0.2) {
                random_text(&mut r, true)
            } else {
                String::new()
            },
        ]);
    }

    let mut products = Table::new(
        "products",
        &[
            "product_id",
            "name",
            "category",
            "subcategory",
            "price",
            "cost",
            "quantity_in_stock",
            "is_discontinued",
            "created_date",
            "description",
        ],
    );
    for i in 1..=s.products {
        let (category, subs) = *r.pick(SUBCATEGORIES);
        let price = r.range(99, 999_999);
        let cost = (price as f64 * (0.5 + 0.4 * r.unit())).round() as i64;
        products.rows.push(vec![
            i.to_string(),
            format!(
                "{} {} {}",
                r.pick(PRODUCT_ADJECTIVES),
                r.pick(PRODUCT_NOUNS),
                i
            ),
            category.to_string(),
            r.pick(subs).to_string(),
            cents_text(price),
            cents_text(cost),
            r.range(0, 10_000).to_string(),
            (r.chance(0.05) as i32).to_string(),
            random_date(&mut r, 2018, 2025),
            if r.chance(0.9) {
                random_text(&mut r, false)
            } else {
                String::new()
            },
        ]);
    }

    let mut orders = Table::new(
        "orders",
        &[
            "order_id",
            "customer_id",
            "order_date",
            "status",
            "total_amount",
            "discount_percent",
            "shipping_method",
            "notes",
        ],
    );
    let top = (s.customers / 5).max(1) as i64;
    for i in 1..=s.orders {
        let customer = if r.chance(0.8) {
            r.range(1, top)
        } else {
            r.range(1, s.customers as i64)
        };
        orders.rows.push(vec![
            i.to_string(),
            customer.to_string(),
            random_date(&mut r, 2023, 2025),
            r.pick(ORDER_STATUSES).to_string(),
            cents_text(r.range(1_000, 500_000)),
            r.pick(&[0, 5, 10, 15, 20, 25]).to_string(),
            r.pick(SHIPPING_METHODS).to_string(),
            if r.chance(0.5) {
                random_text(&mut r, false)
            } else {
                String::new()
            },
        ]);
    }

    let mut items = Table::new(
        "order_items",
        &[
            "item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "line_total",
        ],
    );
    for i in 1..=s.order_items {
        let quantity = r.range(1, 10);
        let unit = r.range(500, 50_000);
        items.rows.push(vec![
            i.to_string(),
            r.range(1, s.orders as i64).to_string(),
            r.range(1, s.products as i64).to_string(),
            quantity.to_string(),
            cents_text(unit),
            cents_text(unit * quantity),
        ]);
    }

    let mut reviews = Table::new(
        "reviews",
        &[
            "review_id",
            "customer_id",
            "product_id",
            "rating",
            "review_date",
            "title",
            "body",
            "helpful_votes",
        ],
    );
    for i in 1..=s.reviews {
        reviews.rows.push(vec![
            i.to_string(),
            r.range(1, s.customers as i64).to_string(),
            r.range(1, s.products as i64).to_string(),
            r.range(1, 5).to_string(),
            random_date(&mut r, 2023, 2025),
            random_text(&mut r, false),
            if r.chance(0.85) {
                random_text(&mut r, true)
            } else {
                String::new()
            },
            r.range(0, 1000).to_string(),
        ]);
    }

    vec![customers, products, orders, items, reviews]
}

/// The key column of each generated table: never salted with edge values.
pub fn is_key_column(column: &str, table: &str) -> bool {
    let key = match table {
        "customers" => "customer_id",
        "products" => "product_id",
        "orders" => "order_id",
        "order_items" => "item_id",
        "reviews" => "review_id",
        _ => return false,
    };
    column == key
}

/// Replace each non-key cell with an edge value with probability `rate`.
pub fn inject_edges(tables: &mut [Table], seed: u64, rate: f64) {
    if rate <= 0.0 {
        return;
    }
    let mut r = Rng::stream(seed, 1);
    for t in tables.iter_mut() {
        let keys: Vec<bool> = t
            .columns
            .iter()
            .map(|c| is_key_column(c, &t.name))
            .collect();
        for (ri, row) in t.rows.iter_mut().enumerate() {
            for (ci, cell) in row.iter_mut().enumerate() {
                if keys[ci] || !r.chance(rate) {
                    continue;
                }
                let v = *r.pick(EDGE_VALUES);
                *cell = v.to_string();
                if v.is_empty() && r.chance(0.5) {
                    t.quoted_empty.insert((ri, ci));
                }
                // One cell in fifty becomes a very long string.
                if r.chance(0.02) {
                    *cell = "long ".repeat(1000);
                }
            }
        }
    }
}

pub struct GenOptions {
    pub output_dir: std::path::PathBuf,
    pub rows: usize,
    pub edge_rate: f64,
    pub format: Format,
}

fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub fn run(seed: u64, opts: &GenOptions, verbose: bool) -> Result<()> {
    let data_dir = opts.output_dir.join("data");
    fs::create_dir_all(&data_dir).with_context(|| format!("create {}", data_dir.display()))?;
    let mut tables = base_tables(seed, opts.rows);
    inject_edges(&mut tables, seed, opts.edge_rate);
    let mut layout = Rng::stream(seed, 2);
    let mut counts = Vec::new();
    for t in &tables {
        let path = write_table(&data_dir, t, &opts.format, &mut layout)?;
        if verbose {
            eprintln!("[tsq] wrote {} ({} rows)", path.display(), t.rows.len());
        }
        counts.push(format!("    {}: {}", json_str(&t.name), t.rows.len()));
    }
    let f = &opts.format;
    let meta = format!(
        "{{\n  \"tsq_version\": {},\n  \"seed\": {},\n  \"rows\": {},\n  \"edge_rate\": {},\n  \
         \"format\": {},\n  \"delimiter\": {},\n  \"crlf\": {},\n  \"bom\": {},\n  \"header\": {},\n  \
         \"ragged_rate\": {},\n  \"row_counts\": {{\n{}\n  }}\n}}\n",
        json_str(env!("CARGO_PKG_VERSION")),
        seed,
        opts.rows,
        opts.edge_rate,
        json_str(f.extension()),
        json_str(&(f.delimiter as char).to_string()),
        f.crlf,
        f.bom,
        f.header,
        f.ragged_rate,
        counts.join(",\n")
    );
    fs::write(opts.output_dir.join("metadata.json"), meta)?;
    println!(
        "tsq gen: seed {} -> {} ({} tables, {} rows)",
        seed,
        data_dir.display(),
        tables.len(),
        tables.iter().map(|t| t.rows.len()).sum::<usize>()
    );
    Ok(())
}

/// Write `tables` as CSV into `dir` (the differential modes' layout).
pub fn write_csv_tables(dir: &Path, tables: &[Table]) -> Result<()> {
    let mut r = Rng::new(0);
    for t in tables {
        write_table(dir, t, &Format::default(), &mut r)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn money_is_canonical_real_text() {
        assert_eq!(cents_text(1250), "12.5");
        assert_eq!(cents_text(700), "7.0");
        assert_eq!(cents_text(5), "0.05");
        assert_eq!(cents_text(1234), "12.34");
    }

    #[test]
    fn distributions_use_the_whole_range() {
        // Regression for the old RNG, whose floats never reached 0.5: every
        // customer was active and nullable columns were never empty.
        let t = &base_tables(3, 2000)[0];
        let active = t.rows.iter().filter(|r| r[7] == "1").count();
        assert!((1600..1800).contains(&active), "active: {active}");
        let notes = t.rows.iter().filter(|r| !r[10].is_empty()).count();
        assert!((300..500).contains(&notes), "notes: {notes}");
        let high_credit = t
            .rows
            .iter()
            .filter(|r| r[8].parse::<i64>().unwrap() > 575)
            .count();
        assert!((900..1100).contains(&high_credit), "credit: {high_credit}");
    }

    #[test]
    fn orders_follow_the_pareto_split() {
        let ts = base_tables(5, 1000);
        let top = ts[2]
            .rows
            .iter()
            .filter(|r| r[1].parse::<usize>().unwrap() <= 200)
            .count();
        // 80% to the top fifth, plus a fifth of the remaining 20%.
        let share = top as f64 / ts[2].rows.len() as f64;
        assert!((0.80..0.88).contains(&share), "share {share}");
    }
}

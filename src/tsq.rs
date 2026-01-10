//! TSQ - Test SQL Query Generator for sqawk
//!
//! Generates deterministic test data and SQL queries for comprehensive sqawk testing.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

// ============================================================================
// CLI Arguments
// ============================================================================

#[derive(Parser, Debug)]
#[clap(name = "tsq", about = "Test SQL Query Generator for sqawk")]
struct Args {
    /// Random seed for reproducible generation (prints seed if not specified)
    #[clap(long, short = 's')]
    seed: Option<u64>,

    /// Number of customer rows (base count, other tables scale proportionally)
    #[clap(long, short = 'r', default_value = "100000")]
    rows: usize,

    /// Output directory for generated files
    #[clap(long, short = 'o')]
    output_dir: String,

    /// Verbose output showing generation progress
    #[clap(long, short = 'v')]
    verbose: bool,
}

// ============================================================================
// Data Pools - Static data for random generation
// ============================================================================

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

const CATEGORIES: &[&str] = &[
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Books",
    "Toys",
    "Automotive",
    "Health",
    "Beauty",
    "Food",
];

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

// ============================================================================
// Verification Data - Tracks counts for verification
// ============================================================================

#[derive(Default)]
struct VerificationData {
    // Row counts
    customer_count: usize,
    product_count: usize,
    order_count: usize,
    order_item_count: usize,
    review_count: usize,

    // Distribution counts
    city_counts: HashMap<String, usize>,
    state_counts: HashMap<String, usize>,
    category_counts: HashMap<String, usize>,
    status_counts: HashMap<String, usize>,
    shipping_counts: HashMap<String, usize>,
    rating_counts: HashMap<i32, usize>,

    // NULL counts
    customers_with_notes: usize,
    reviews_with_body: usize,

    // Active/inactive counts
    active_customers: usize,
    discontinued_products: usize,

    // Aggregate values
    total_order_amount: f64,
    total_product_price: f64,

    // Credit score distribution
    credit_below_400: usize,
    credit_600_to_750: usize,
    credit_above_700: usize,
    credit_above_800: usize,
    min_credit_score: i32,
    max_credit_score: i32,

    // Price distribution
    price_below_100: usize,

    // Date ranges
    min_order_date: String,
    max_order_date: String,

    // Customer orders distribution
    customers_with_orders: usize,
}

// ============================================================================
// Data Generator
// ============================================================================

struct DataGenerator {
    rng: StdRng,
    row_count: usize,
    verbose: bool,
    verification: VerificationData,
}

impl DataGenerator {
    fn new(seed: u64, row_count: usize, verbose: bool) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            row_count,
            verbose,
            verification: VerificationData {
                min_credit_score: i32::MAX,
                max_credit_score: i32::MIN,
                min_order_date: "9999-12-31".to_string(),
                max_order_date: "0000-01-01".to_string(),
                ..Default::default()
            },
        }
    }

    fn log(&self, msg: &str) {
        if self.verbose {
            eprintln!("[tsq] {}", msg);
        }
    }

    fn escape_csv(value: &str) -> String {
        if value.contains(',')
            || value.contains('"')
            || value.contains('\n')
            || value.contains('\r')
        {
            format!("\"{}\"", value.replace('"', "\"\""))
        } else {
            value.to_string()
        }
    }

    fn random_date(&mut self, year_start: i32, year_end: i32) -> String {
        let year = self.rng.gen_range(year_start..=year_end);
        let month = self.rng.gen_range(1..=12);
        let day = match month {
            2 => self.rng.gen_range(1..=28),
            4 | 6 | 9 | 11 => self.rng.gen_range(1..=30),
            _ => self.rng.gen_range(1..=31),
        };
        format!("{:04}-{:02}-{:02}", year, month, day)
    }

    fn random_text(&mut self, with_special_chars: bool) -> String {
        let words: Vec<&str> = vec![
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
        let word_count = self.rng.gen_range(3..=10);
        let mut text: Vec<String> = (0..word_count)
            .map(|_| words[self.rng.gen_range(0..words.len())].to_string())
            .collect();

        if with_special_chars && self.rng.gen_bool(0.1) {
            // Add some special characters occasionally
            let specials = ["can't", "won't", "it's", "O'Brien", "Smith & Co"];
            text.push(specials[self.rng.gen_range(0..specials.len())].to_string());
        }

        text.join(" ")
    }

    fn generate_customers(&mut self, path: &Path) -> Result<()> {
        self.log(&format!("Generating {} customers...", self.row_count));

        let file = File::create(path).context("Failed to create customers.csv")?;
        let mut writer = BufWriter::new(file);

        // Header
        writeln!(
            writer,
            "customer_id,name,email,city,state,country,signup_date,is_active,credit_score,notes"
        )?;

        for i in 1..=self.row_count {
            let first = FIRST_NAMES[self.rng.gen_range(0..FIRST_NAMES.len())];
            let last = LAST_NAMES[self.rng.gen_range(0..LAST_NAMES.len())];
            let name = format!("{} {}", first, last);

            let email_domain = EMAIL_DOMAINS[self.rng.gen_range(0..EMAIL_DOMAINS.len())];
            let email = format!(
                "{}.{}{}@{}",
                first.to_lowercase(),
                last.to_lowercase(),
                i % 1000,
                email_domain
            );

            let (city, state) = CITIES[self.rng.gen_range(0..CITIES.len())];
            let country = COUNTRIES[self.rng.gen_range(0..COUNTRIES.len())];
            let signup_date = self.random_date(2020, 2025);
            let is_active = if self.rng.gen_bool(0.85) { 1 } else { 0 };
            let credit_score = self.rng.gen_range(300..=850);

            // ~20% have notes
            let notes = if self.rng.gen_bool(0.2) {
                self.verification.customers_with_notes += 1;
                Self::escape_csv(&self.random_text(true))
            } else {
                String::new()
            };

            // Track verification data
            *self
                .verification
                .city_counts
                .entry(city.to_string())
                .or_insert(0) += 1;
            *self
                .verification
                .state_counts
                .entry(state.to_string())
                .or_insert(0) += 1;

            if is_active == 1 {
                self.verification.active_customers += 1;
            }

            if credit_score < 400 {
                self.verification.credit_below_400 += 1;
            }
            if (600..=750).contains(&credit_score) {
                self.verification.credit_600_to_750 += 1;
            }
            if credit_score > 700 {
                self.verification.credit_above_700 += 1;
            }
            if credit_score > 800 {
                self.verification.credit_above_800 += 1;
            }
            self.verification.min_credit_score =
                self.verification.min_credit_score.min(credit_score);
            self.verification.max_credit_score =
                self.verification.max_credit_score.max(credit_score);

            writeln!(
                writer,
                "{},{},{},{},{},{},{},{},{},{}",
                i,
                Self::escape_csv(&name),
                email,
                Self::escape_csv(city),
                state,
                country,
                signup_date,
                is_active,
                credit_score,
                notes
            )?;
        }

        self.verification.customer_count = self.row_count;
        self.log(&format!("  Created {} customers", self.row_count));
        Ok(())
    }

    fn generate_products(&mut self, path: &Path) -> Result<()> {
        let count = (self.row_count / 100).max(100);
        self.log(&format!("Generating {} products...", count));

        let file = File::create(path).context("Failed to create products.csv")?;
        let mut writer = BufWriter::new(file);

        // Header
        writeln!(
            writer,
            "product_id,name,category,subcategory,price,cost,quantity_in_stock,is_discontinued,created_date,description"
        )?;

        // Build subcategory lookup
        let subcategory_map: HashMap<&str, &[&str]> = SUBCATEGORIES.iter().cloned().collect();

        for i in 1..=count {
            let adj = PRODUCT_ADJECTIVES[self.rng.gen_range(0..PRODUCT_ADJECTIVES.len())];
            let noun = PRODUCT_NOUNS[self.rng.gen_range(0..PRODUCT_NOUNS.len())];
            let name = format!("{} {} {}", adj, noun, i);

            let category = CATEGORIES[self.rng.gen_range(0..CATEGORIES.len())];
            let subcats = subcategory_map.get(category).unwrap();
            let subcategory = subcats[self.rng.gen_range(0..subcats.len())];

            let price: f64 = self.rng.gen_range(0.99..=9999.99);
            let price = (price * 100.0).round() / 100.0;
            let cost = (price * self.rng.gen_range(0.5..=0.9) * 100.0).round() / 100.0;
            let quantity_in_stock = self.rng.gen_range(0..=10000);
            let is_discontinued = if self.rng.gen_bool(0.05) { 1 } else { 0 };
            let created_date = self.random_date(2018, 2025);

            // ~10% have description
            let description = if self.rng.gen_bool(0.9) {
                Self::escape_csv(&self.random_text(false))
            } else {
                String::new()
            };

            // Track verification data
            *self
                .verification
                .category_counts
                .entry(category.to_string())
                .or_insert(0) += 1;
            self.verification.total_product_price += price;

            if price < 100.0 {
                self.verification.price_below_100 += 1;
            }
            if is_discontinued == 1 {
                self.verification.discontinued_products += 1;
            }

            writeln!(
                writer,
                "{},{},{},{},{:.2},{:.2},{},{},{},{}",
                i,
                Self::escape_csv(&name),
                Self::escape_csv(category),
                Self::escape_csv(subcategory),
                price,
                cost,
                quantity_in_stock,
                is_discontinued,
                created_date,
                description
            )?;
        }

        self.verification.product_count = count;
        self.log(&format!("  Created {} products", count));
        Ok(())
    }

    fn generate_orders(&mut self, path: &Path) -> Result<Vec<(usize, usize, f64)>> {
        let count = self.row_count * 3;
        self.log(&format!("Generating {} orders...", count));

        let file = File::create(path).context("Failed to create orders.csv")?;
        let mut writer = BufWriter::new(file);

        // Header
        writeln!(
            writer,
            "order_id,customer_id,order_date,status,total_amount,discount_percent,shipping_method,notes"
        )?;

        // Track which customers have orders
        let mut customer_order_counts: HashMap<usize, usize> = HashMap::new();

        // Store order info for order_items generation
        let mut order_info: Vec<(usize, usize, f64)> = Vec::with_capacity(count);

        for i in 1..=count {
            // Pareto distribution: 80% of orders from 20% of customers
            let customer_id = if self.rng.gen_bool(0.8) {
                // Top 20% of customers
                self.rng.gen_range(1..=(self.row_count / 5).max(1))
            } else {
                self.rng.gen_range(1..=self.row_count)
            };

            *customer_order_counts.entry(customer_id).or_insert(0) += 1;

            let order_date = self.random_date(2023, 2025);
            let status = ORDER_STATUSES[self.rng.gen_range(0..ORDER_STATUSES.len())];
            let total_amount: f64 = self.rng.gen_range(10.0..=5000.0);
            let total_amount = (total_amount * 100.0).round() / 100.0;
            let discount_percent = [0, 5, 10, 15, 20, 25][self.rng.gen_range(0..6)];
            let shipping_method = SHIPPING_METHODS[self.rng.gen_range(0..SHIPPING_METHODS.len())];

            // ~50% have notes
            let notes = if self.rng.gen_bool(0.5) {
                Self::escape_csv(&self.random_text(false))
            } else {
                String::new()
            };

            // Track verification data
            *self
                .verification
                .status_counts
                .entry(status.to_string())
                .or_insert(0) += 1;
            *self
                .verification
                .shipping_counts
                .entry(shipping_method.to_string())
                .or_insert(0) += 1;
            self.verification.total_order_amount += total_amount;

            if order_date < self.verification.min_order_date {
                self.verification.min_order_date = order_date.clone();
            }
            if order_date > self.verification.max_order_date {
                self.verification.max_order_date = order_date.clone();
            }

            order_info.push((i, customer_id, total_amount));

            writeln!(
                writer,
                "{},{},{},{},{:.2},{},{},{}",
                i,
                customer_id,
                order_date,
                status,
                total_amount,
                discount_percent,
                shipping_method,
                notes
            )?;
        }

        self.verification.order_count = count;
        self.verification.customers_with_orders = customer_order_counts.len();
        self.log(&format!("  Created {} orders", count));
        Ok(order_info)
    }

    fn generate_order_items(
        &mut self,
        path: &Path,
        order_info: &[(usize, usize, f64)],
    ) -> Result<()> {
        let count = self.row_count * 10;
        self.log(&format!("Generating {} order items...", count));

        let file = File::create(path).context("Failed to create order_items.csv")?;
        let mut writer = BufWriter::new(file);

        // Header
        writeln!(
            writer,
            "item_id,order_id,product_id,quantity,unit_price,line_total"
        )?;

        let product_count = self.verification.product_count;
        let order_count = order_info.len();

        for i in 1..=count {
            let order_id = self.rng.gen_range(1..=order_count);
            let product_id = self.rng.gen_range(1..=product_count);
            let quantity = self.rng.gen_range(1..=10);
            let unit_price: f64 = self.rng.gen_range(5.0..=500.0);
            let unit_price = (unit_price * 100.0).round() / 100.0;
            let line_total = (quantity as f64 * unit_price * 100.0).round() / 100.0;

            writeln!(
                writer,
                "{},{},{},{},{:.2},{:.2}",
                i, order_id, product_id, quantity, unit_price, line_total
            )?;
        }

        self.verification.order_item_count = count;
        self.log(&format!("  Created {} order items", count));
        Ok(())
    }

    fn generate_reviews(&mut self, path: &Path) -> Result<()> {
        let count = self.row_count / 2;
        self.log(&format!("Generating {} reviews...", count));

        let file = File::create(path).context("Failed to create reviews.csv")?;
        let mut writer = BufWriter::new(file);

        // Header
        writeln!(
            writer,
            "review_id,customer_id,product_id,rating,review_date,title,body,helpful_votes"
        )?;

        let product_count = self.verification.product_count;

        for i in 1..=count {
            let customer_id = self.rng.gen_range(1..=self.row_count);
            let product_id = self.rng.gen_range(1..=product_count);
            let rating = self.rng.gen_range(1..=5);
            let review_date = self.random_date(2023, 2025);
            let title = Self::escape_csv(&self.random_text(false));

            // ~85% have body
            let body = if self.rng.gen_bool(0.85) {
                self.verification.reviews_with_body += 1;
                Self::escape_csv(&self.random_text(true))
            } else {
                String::new()
            };

            let helpful_votes = self.rng.gen_range(0..=1000);

            // Track verification data
            *self.verification.rating_counts.entry(rating).or_insert(0) += 1;

            writeln!(
                writer,
                "{},{},{},{},{},{},{},{}",
                i, customer_id, product_id, rating, review_date, title, body, helpful_votes
            )?;
        }

        self.verification.review_count = count;
        self.log(&format!("  Created {} reviews", count));
        Ok(())
    }

    fn generate_all(&mut self, base_path: &Path) -> Result<()> {
        let data_path = base_path.join("data");

        self.generate_customers(&data_path.join("customers.csv"))?;
        self.generate_products(&data_path.join("products.csv"))?;
        let order_info = self.generate_orders(&data_path.join("orders.csv"))?;
        self.generate_order_items(&data_path.join("order_items.csv"), &order_info)?;
        self.generate_reviews(&data_path.join("reviews.csv"))?;

        Ok(())
    }
}

// ============================================================================
// Query Generator
// ============================================================================

struct QueryGenerator<'a> {
    verification: &'a VerificationData,
}

impl<'a> QueryGenerator<'a> {
    fn new(verification: &'a VerificationData) -> Self {
        Self { verification }
    }

    fn write_query_file(&self, path: &Path, filename: &str, content: &str) -> Result<()> {
        let file_path = path.join(filename);
        let mut file = File::create(&file_path)
            .with_context(|| format!("Failed to create {}", file_path.display()))?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }

    fn generate_all(&self, base_path: &Path) -> Result<()> {
        let queries_path = base_path.join("queries");

        self.write_query_file(
            &queries_path,
            "01_select_basic.sql",
            &self.gen_select_basic(),
        )?;
        self.write_query_file(
            &queries_path,
            "02_where_comparison.sql",
            &self.gen_where_comparison(),
        )?;
        self.write_query_file(
            &queries_path,
            "03_where_logical.sql",
            &self.gen_where_logical(),
        )?;
        self.write_query_file(
            &queries_path,
            "04_where_pattern.sql",
            &self.gen_where_pattern(),
        )?;
        self.write_query_file(&queries_path, "05_join_inner.sql", &self.gen_join_inner())?;
        self.write_query_file(&queries_path, "06_join_multi.sql", &self.gen_join_multi())?;
        self.write_query_file(
            &queries_path,
            "07_aggregate_basic.sql",
            &self.gen_aggregate_basic(),
        )?;
        self.write_query_file(
            &queries_path,
            "08_groupby_having.sql",
            &self.gen_groupby_having(),
        )?;
        self.write_query_file(&queries_path, "09_orderby.sql", &self.gen_orderby())?;
        self.write_query_file(
            &queries_path,
            "10_limit_offset.sql",
            &self.gen_limit_offset(),
        )?;
        self.write_query_file(&queries_path, "11_distinct.sql", &self.gen_distinct())?;
        self.write_query_file(&queries_path, "12_window.sql", &self.gen_window())?;
        self.write_query_file(
            &queries_path,
            "13_subquery_scalar.sql",
            &self.gen_subquery_scalar(),
        )?;
        self.write_query_file(&queries_path, "14_subquery_in.sql", &self.gen_subquery_in())?;
        self.write_query_file(
            &queries_path,
            "15_subquery_exists.sql",
            &self.gen_subquery_exists(),
        )?;
        self.write_query_file(
            &queries_path,
            "16_subquery_correlated.sql",
            &self.gen_subquery_correlated(),
        )?;
        self.write_query_file(&queries_path, "17_setop_union.sql", &self.gen_setop_union())?;
        self.write_query_file(
            &queries_path,
            "18_setop_intersect_except.sql",
            &self.gen_setop_intersect_except(),
        )?;
        self.write_query_file(
            &queries_path,
            "19_string_functions.sql",
            &self.gen_string_functions(),
        )?;
        self.write_query_file(
            &queries_path,
            "20_math_functions.sql",
            &self.gen_math_functions(),
        )?;
        self.write_query_file(
            &queries_path,
            "21_case_coalesce.sql",
            &self.gen_case_coalesce(),
        )?;
        self.write_query_file(
            &queries_path,
            "22_mutation_insert.sql",
            &self.gen_mutation_insert(),
        )?;
        self.write_query_file(
            &queries_path,
            "23_mutation_update.sql",
            &self.gen_mutation_update(),
        )?;
        self.write_query_file(
            &queries_path,
            "24_mutation_delete.sql",
            &self.gen_mutation_delete(),
        )?;
        self.write_query_file(
            &queries_path,
            "25_complex_combined.sql",
            &self.gen_complex_combined(),
        )?;

        Ok(())
    }

    fn gen_select_basic(&self) -> String {
        r#"-- 01_select_basic.sql - Basic SELECT queries
-- Generated by tsq

-- Q001: Select all columns with LIMIT
SELECT * FROM customers LIMIT 10;
-- EXPECTED_COUNT: 10

-- Q002: Select specific columns
SELECT customer_id, name, email FROM customers LIMIT 10;
-- EXPECTED_COUNT: 10

-- Q003: Select with column alias
SELECT customer_id AS id, name AS customer_name FROM customers LIMIT 5;
-- EXPECTED_COUNT: 5

-- Q004: Select all from products
SELECT * FROM products LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q005: Select from orders
SELECT order_id, customer_id, total_amount FROM orders LIMIT 15;
-- EXPECTED_COUNT: 15
"#
        .to_string()
    }

    fn gen_where_comparison(&self) -> String {
        let v = &self.verification;
        // Pick a city that exists
        let city = v
            .city_counts
            .keys()
            .next()
            .map(|s| s.as_str())
            .unwrap_or("New York");
        let city_count = v.city_counts.get(city).copied().unwrap_or(0);

        format!(
            r#"-- 02_where_comparison.sql - WHERE clause comparison operators
-- Generated by tsq

-- Q010: Equals comparison (city)
SELECT * FROM customers WHERE city = '{city}';
-- EXPECTED_COUNT: {city_count}

-- Q011: Not equals
SELECT COUNT(*) FROM customers WHERE country != 'USA';
-- EXPECTED_COUNT: 1

-- Q012: Less than
SELECT COUNT(*) FROM products WHERE price < 100.00;
-- EXPECTED_COUNT: 1

-- Q013: Greater than
SELECT COUNT(*) FROM customers WHERE credit_score > 700;
-- EXPECTED_COUNT: 1

-- Q014: Greater than or equal
SELECT COUNT(*) FROM customers WHERE credit_score >= 800;
-- EXPECTED_COUNT: 1

-- Q015: Less than or equal
SELECT COUNT(*) FROM customers WHERE credit_score <= 400;
-- EXPECTED_COUNT: 1

-- Q016: IS NULL
SELECT COUNT(*) FROM customers WHERE notes IS NULL;
-- EXPECTED_COUNT: 1

-- Q017: IS NOT NULL
SELECT COUNT(*) FROM customers WHERE notes IS NOT NULL;
-- EXPECTED_COUNT: 1

-- Q018: Combined comparison
SELECT * FROM products WHERE price >= 50.00 AND price <= 200.00 LIMIT 20;
-- EXPECTED_COUNT: 20
"#,
            city = city,
            city_count = city_count
        )
    }

    fn gen_where_logical(&self) -> String {
        r#"-- 03_where_logical.sql - WHERE clause logical operators
-- Generated by tsq

-- Q020: AND condition
SELECT COUNT(*) FROM customers WHERE is_active = 1 AND credit_score > 700;
-- EXPECTED_COUNT: 1

-- Q021: OR condition
SELECT COUNT(*) FROM orders WHERE status = 'cancelled' OR status = 'returned';
-- EXPECTED_COUNT: 1

-- Q022: NOT condition
SELECT COUNT(*) FROM products WHERE NOT is_discontinued = 1;
-- EXPECTED_COUNT: 1

-- Q023: Complex AND/OR with parentheses
SELECT COUNT(*) FROM customers WHERE (city = 'New York' OR city = 'Los Angeles') AND is_active = 1;
-- EXPECTED_COUNT: 1

-- Q024: Multiple AND
SELECT COUNT(*) FROM orders WHERE status = 'delivered' AND discount_percent > 0 AND total_amount > 100;
-- EXPECTED_COUNT: 1

-- Q025: NOT with comparison
SELECT COUNT(*) FROM customers WHERE NOT credit_score < 600;
-- EXPECTED_COUNT: 1
"#.to_string()
    }

    fn gen_where_pattern(&self) -> String {
        r#"-- 04_where_pattern.sql - Pattern matching and IN/BETWEEN
-- Generated by tsq

-- Q030: LIKE with prefix
SELECT COUNT(*) FROM customers WHERE email LIKE 'john%';
-- EXPECTED_COUNT: 1

-- Q031: LIKE with suffix
SELECT COUNT(*) FROM customers WHERE email LIKE '%@gmail.com';
-- EXPECTED_COUNT: 1

-- Q032: LIKE with contains
SELECT COUNT(*) FROM products WHERE name LIKE '%Pro%';
-- EXPECTED_COUNT: 1

-- Q033: IN list integers
SELECT COUNT(*) FROM reviews WHERE rating IN (4, 5);
-- EXPECTED_COUNT: 1

-- Q034: IN list strings
SELECT COUNT(*) FROM orders WHERE status IN ('pending', 'shipped');
-- EXPECTED_COUNT: 1

-- Q035: NOT IN
SELECT COUNT(*) FROM orders WHERE shipping_method NOT IN ('overnight', 'express');
-- EXPECTED_COUNT: 1

-- Q036: BETWEEN numeric
SELECT COUNT(*) FROM customers WHERE credit_score BETWEEN 600 AND 750;
-- EXPECTED_COUNT: 1

-- Q037: NOT BETWEEN
SELECT COUNT(*) FROM products WHERE price NOT BETWEEN 10.00 AND 100.00;
-- EXPECTED_COUNT: 1
"#
        .to_string()
    }

    fn gen_join_inner(&self) -> String {
        r#"-- 05_join_inner.sql - Two-table INNER JOIN queries
-- Generated by tsq

-- Q040: Basic two-table join (implicit)
SELECT c.name, o.order_id, o.total_amount
FROM customers c, orders o
WHERE c.customer_id = o.customer_id
LIMIT 100;
-- EXPECTED_COUNT: 100

-- Q041: Join with additional filter
SELECT c.name, o.order_id, o.status
FROM customers c, orders o
WHERE c.customer_id = o.customer_id AND o.status = 'delivered'
LIMIT 50;
-- EXPECTED_COUNT: 50

-- Q042: Join with aggregate
SELECT c.customer_id, c.name, COUNT(*) AS order_count
FROM customers c, orders o
WHERE c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q043: Products and order items join
SELECT p.name, oi.quantity, oi.unit_price
FROM products p, order_items oi
WHERE p.product_id = oi.product_id
LIMIT 50;
-- EXPECTED_COUNT: 50
"#
        .to_string()
    }

    fn gen_join_multi(&self) -> String {
        r#"-- 06_join_multi.sql - Multi-table JOIN queries
-- Generated by tsq

-- Q050: Three-table join
SELECT c.name, o.order_id, oi.quantity
FROM customers c, orders o, order_items oi
WHERE c.customer_id = o.customer_id
  AND o.order_id = oi.order_id
LIMIT 100;
-- EXPECTED_COUNT: 100

-- Q051: Four-table join
SELECT c.name, o.order_date, p.name AS product_name, oi.quantity
FROM customers c, orders o, order_items oi, products p
WHERE c.customer_id = o.customer_id
  AND o.order_id = oi.order_id
  AND oi.product_id = p.product_id
LIMIT 50;
-- EXPECTED_COUNT: 50

-- Q052: Three-table join with aggregate
SELECT c.city, COUNT(DISTINCT o.order_id) AS order_count, SUM(oi.line_total) AS total_value
FROM customers c, orders o, order_items oi
WHERE c.customer_id = o.customer_id
  AND o.order_id = oi.order_id
GROUP BY c.city
LIMIT 20;
-- EXPECTED_COUNT: 20
"#
        .to_string()
    }

    fn gen_aggregate_basic(&self) -> String {
        let v = &self.verification;
        format!(
            r#"-- 07_aggregate_basic.sql - Basic aggregate functions
-- Generated by tsq

-- Q060: COUNT(*)
SELECT COUNT(*) AS total_customers FROM customers;
-- EXPECTED_COUNT: 1
-- EXPECTED_VALUE: {customer_count}

-- Q061: COUNT(column) - excludes NULL
SELECT COUNT(notes) AS customers_with_notes FROM customers;
-- EXPECTED_COUNT: 1
-- EXPECTED_VALUE: {customers_with_notes}

-- Q062: SUM
SELECT SUM(total_amount) AS total_revenue FROM orders;
-- EXPECTED_COUNT: 1

-- Q063: AVG
SELECT AVG(credit_score) AS avg_credit FROM customers;
-- EXPECTED_COUNT: 1

-- Q064: MIN
SELECT MIN(price) AS min_price FROM products;
-- EXPECTED_COUNT: 1

-- Q065: MAX
SELECT MAX(price) AS max_price FROM products;
-- EXPECTED_COUNT: 1

-- Q066: Multiple aggregates
SELECT COUNT(*) AS cnt, SUM(quantity) AS total_qty, AVG(unit_price) AS avg_price
FROM order_items;
-- EXPECTED_COUNT: 1

-- Q067: MIN/MAX together
SELECT MIN(credit_score) AS min_credit, MAX(credit_score) AS max_credit FROM customers;
-- EXPECTED_COUNT: 1
"#,
            customer_count = v.customer_count,
            customers_with_notes = v.customers_with_notes
        )
    }

    fn gen_groupby_having(&self) -> String {
        let v = &self.verification;
        let num_cities = v.city_counts.len();
        let num_categories = v.category_counts.len();

        format!(
            r#"-- 08_groupby_having.sql - GROUP BY and HAVING
-- Generated by tsq

-- Q070: Simple GROUP BY
SELECT city, COUNT(*) AS customer_count
FROM customers
GROUP BY city;
-- EXPECTED_COUNT: {num_cities}

-- Q071: GROUP BY with multiple aggregates
SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price, SUM(quantity_in_stock) AS total_stock
FROM products
GROUP BY category;
-- EXPECTED_COUNT: {num_categories}

-- Q072: GROUP BY with HAVING
SELECT city, COUNT(*) AS cnt
FROM customers
GROUP BY city
HAVING COUNT(*) > 100;
-- EXPECTED_COUNT varies

-- Q073: GROUP BY with HAVING on SUM
SELECT customer_id, SUM(total_amount) AS total_spent
FROM orders
GROUP BY customer_id
HAVING SUM(total_amount) > 1000
LIMIT 50;
-- EXPECTED_COUNT: 50

-- Q074: GROUP BY multiple columns
SELECT city, state, COUNT(*) AS cnt
FROM customers
GROUP BY city, state;
-- EXPECTED_COUNT varies

-- Q075: GROUP BY with ORDER BY
SELECT status, COUNT(*) AS cnt
FROM orders
GROUP BY status
ORDER BY cnt DESC;
-- EXPECTED_COUNT: 5
"#,
            num_cities = num_cities,
            num_categories = num_categories
        )
    }

    fn gen_orderby(&self) -> String {
        r#"-- 09_orderby.sql - ORDER BY queries
-- Generated by tsq

-- Q080: ORDER BY single column ASC
SELECT * FROM products ORDER BY price ASC LIMIT 10;
-- EXPECTED_COUNT: 10

-- Q081: ORDER BY single column DESC
SELECT * FROM customers ORDER BY credit_score DESC LIMIT 10;
-- EXPECTED_COUNT: 10

-- Q082: ORDER BY multiple columns
SELECT * FROM orders ORDER BY status ASC, total_amount DESC LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q083: ORDER BY with WHERE
SELECT * FROM customers WHERE is_active = 1 ORDER BY credit_score DESC LIMIT 15;
-- EXPECTED_COUNT: 15

-- Q084: ORDER BY with GROUP BY
SELECT category, AVG(price) AS avg_price
FROM products
GROUP BY category
ORDER BY avg_price DESC;
-- EXPECTED_COUNT varies
"#
        .to_string()
    }

    fn gen_limit_offset(&self) -> String {
        r#"-- 10_limit_offset.sql - LIMIT and OFFSET
-- Generated by tsq

-- Q090: LIMIT only
SELECT * FROM customers LIMIT 50;
-- EXPECTED_COUNT: 50

-- Q091: LIMIT with OFFSET
SELECT * FROM customers LIMIT 25 OFFSET 100;
-- EXPECTED_COUNT: 25

-- Q092: Large OFFSET
SELECT * FROM orders LIMIT 10 OFFSET 1000;
-- EXPECTED_COUNT: 10

-- Q093: LIMIT 1
SELECT * FROM products ORDER BY price DESC LIMIT 1;
-- EXPECTED_COUNT: 1

-- Q094: LIMIT with ORDER BY and WHERE
SELECT * FROM customers WHERE is_active = 1 ORDER BY credit_score DESC LIMIT 20 OFFSET 10;
-- EXPECTED_COUNT: 20
"#
        .to_string()
    }

    fn gen_distinct(&self) -> String {
        let v = &self.verification;
        let num_cities = v.city_counts.len();
        let num_categories = v.category_counts.len();

        format!(
            r#"-- 11_distinct.sql - DISTINCT queries
-- Generated by tsq

-- Q100: DISTINCT single column
SELECT DISTINCT city FROM customers;
-- EXPECTED_COUNT: {num_cities}

-- Q101: DISTINCT multiple columns
SELECT DISTINCT city, state FROM customers;
-- EXPECTED_COUNT varies

-- Q102: DISTINCT with ORDER BY
SELECT DISTINCT category FROM products ORDER BY category ASC;
-- EXPECTED_COUNT: {num_categories}

-- Q103: DISTINCT with WHERE
SELECT DISTINCT status FROM orders WHERE total_amount > 500;
-- EXPECTED_COUNT varies

-- Q104: DISTINCT on joined tables
SELECT DISTINCT c.city
FROM customers c, orders o
WHERE c.customer_id = o.customer_id AND o.status = 'delivered';
-- EXPECTED_COUNT varies
"#,
            num_cities = num_cities,
            num_categories = num_categories
        )
    }

    fn gen_window(&self) -> String {
        r#"-- 12_window.sql - Window functions
-- Generated by tsq

-- Q110: ROW_NUMBER without partition
SELECT customer_id, name, credit_score,
       ROW_NUMBER() OVER (ORDER BY credit_score DESC) AS rank
FROM customers LIMIT 10;
-- EXPECTED_COUNT: 10

-- Q111: ROW_NUMBER with PARTITION BY
SELECT product_id, category, price,
       ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS cat_rank
FROM products LIMIT 50;
-- EXPECTED_COUNT: 50

-- Q112: RANK
SELECT review_id, rating,
       RANK() OVER (ORDER BY rating DESC) AS rating_rank
FROM reviews LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q113: DENSE_RANK
SELECT customer_id, credit_score,
       DENSE_RANK() OVER (ORDER BY credit_score DESC) AS dense_rank
FROM customers LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q114: SUM OVER (running total)
SELECT order_id, total_amount,
       SUM(total_amount) OVER (ORDER BY order_id) AS running_total
FROM orders LIMIT 10;
-- EXPECTED_COUNT: 10

-- Q115: AVG OVER with PARTITION
SELECT product_id, category, price,
       AVG(price) OVER (PARTITION BY category) AS category_avg
FROM products LIMIT 50;
-- EXPECTED_COUNT: 50
"#
        .to_string()
    }

    fn gen_subquery_scalar(&self) -> String {
        r#"-- 13_subquery_scalar.sql - Scalar subqueries
-- Generated by tsq

-- Q120: Scalar subquery with MAX
SELECT * FROM customers
WHERE credit_score = (SELECT MAX(credit_score) FROM customers);
-- EXPECTED_COUNT varies (ties possible)

-- Q121: Scalar subquery with AVG comparison
SELECT COUNT(*) FROM products
WHERE price > (SELECT AVG(price) FROM products);
-- EXPECTED_COUNT: 1

-- Q122: Scalar subquery with MIN
SELECT * FROM products
WHERE price = (SELECT MIN(price) FROM products);
-- EXPECTED_COUNT varies (ties possible)

-- Q123: Nested scalar in SELECT (if supported)
SELECT customer_id, name,
       (SELECT COUNT(*) FROM customers) AS total_customers
FROM customers LIMIT 5;
-- EXPECTED_COUNT: 5
"#
        .to_string()
    }

    fn gen_subquery_in(&self) -> String {
        r#"-- 14_subquery_in.sql - IN subqueries
-- Generated by tsq

-- Q130: IN subquery
SELECT * FROM customers
WHERE customer_id IN (SELECT DISTINCT customer_id FROM orders WHERE status = 'delivered')
LIMIT 100;
-- EXPECTED_COUNT: 100

-- Q131: NOT IN subquery
SELECT COUNT(*) FROM products
WHERE product_id NOT IN (SELECT DISTINCT product_id FROM order_items);
-- EXPECTED_COUNT: 1

-- Q132: IN subquery with aggregate filter
SELECT * FROM customers
WHERE customer_id IN (
    SELECT customer_id FROM orders
    GROUP BY customer_id
    HAVING COUNT(*) > 5
)
LIMIT 50;
-- EXPECTED_COUNT: 50
"#
        .to_string()
    }

    fn gen_subquery_exists(&self) -> String {
        r#"-- 15_subquery_exists.sql - EXISTS subqueries
-- Generated by tsq

-- Q140: EXISTS
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
LIMIT 100;
-- EXPECTED_COUNT: 100

-- Q141: NOT EXISTS
SELECT COUNT(*) FROM products p
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.product_id);
-- EXPECTED_COUNT: 1

-- Q142: EXISTS with additional condition
SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.customer_id AND o.status = 'delivered'
)
LIMIT 50;
-- EXPECTED_COUNT: 50
"#
        .to_string()
    }

    fn gen_subquery_correlated(&self) -> String {
        r#"-- 16_subquery_correlated.sql - Correlated subqueries
-- Generated by tsq

-- Q150: Correlated subquery in WHERE
SELECT * FROM orders o
WHERE o.total_amount > (
    SELECT AVG(o2.total_amount) FROM orders o2 WHERE o2.customer_id = o.customer_id
)
LIMIT 100;
-- EXPECTED_COUNT: 100

-- Q151: Correlated subquery with COUNT
SELECT c.customer_id, c.name
FROM customers c
WHERE (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) > 3
LIMIT 50;
-- EXPECTED_COUNT: 50
"#
        .to_string()
    }

    fn gen_setop_union(&self) -> String {
        r#"-- 17_setop_union.sql - UNION operations
-- Generated by tsq

-- Q160: UNION ALL
SELECT customer_id, name FROM customers WHERE city = 'New York'
UNION ALL
SELECT customer_id, name FROM customers WHERE city = 'Los Angeles'
LIMIT 100;
-- EXPECTED_COUNT varies

-- Q161: UNION (removes duplicates)
SELECT city FROM customers WHERE state = 'CA'
UNION
SELECT city FROM customers WHERE state = 'NY';
-- EXPECTED_COUNT varies

-- Q162: UNION ALL with different filters
SELECT order_id, total_amount FROM orders WHERE status = 'pending'
UNION ALL
SELECT order_id, total_amount FROM orders WHERE status = 'shipped'
LIMIT 200;
-- EXPECTED_COUNT varies
"#
        .to_string()
    }

    fn gen_setop_intersect_except(&self) -> String {
        r#"-- 18_setop_intersect_except.sql - INTERSECT and EXCEPT
-- Generated by tsq

-- Q170: INTERSECT
SELECT customer_id FROM orders WHERE status = 'delivered'
INTERSECT
SELECT customer_id FROM reviews WHERE rating >= 4
LIMIT 50;
-- EXPECTED_COUNT varies

-- Q171: EXCEPT
SELECT customer_id FROM customers WHERE is_active = 1
EXCEPT
SELECT customer_id FROM orders WHERE status = 'cancelled'
LIMIT 100;
-- EXPECTED_COUNT varies

-- Q172: EXCEPT to find customers without orders
SELECT customer_id FROM customers
EXCEPT
SELECT DISTINCT customer_id FROM orders
LIMIT 50;
-- EXPECTED_COUNT varies
"#
        .to_string()
    }

    fn gen_string_functions(&self) -> String {
        r#"-- 19_string_functions.sql - String functions
-- Generated by tsq

-- Q180: UPPER
SELECT * FROM customers WHERE UPPER(city) = 'NEW YORK' LIMIT 50;
-- EXPECTED_COUNT varies

-- Q181: LOWER
SELECT * FROM products WHERE LOWER(category) = 'electronics' LIMIT 50;
-- EXPECTED_COUNT varies

-- Q182: SUBSTR
SELECT * FROM customers WHERE SUBSTR(email, 1, 4) = 'john' LIMIT 20;
-- EXPECTED_COUNT varies

-- Q183: CONCAT
SELECT customer_id, CONCAT(city, ', ', state) AS location FROM customers LIMIT 10;
-- EXPECTED_COUNT: 10

-- Q184: LEFT
SELECT * FROM customers WHERE LEFT(name, 1) = 'J' LIMIT 50;
-- EXPECTED_COUNT varies

-- Q185: RIGHT
SELECT * FROM customers WHERE RIGHT(email, 10) = '@gmail.com' LIMIT 50;
-- EXPECTED_COUNT varies

-- Q186: TRIM
SELECT customer_id, TRIM(city) AS trimmed_city FROM customers LIMIT 10;
-- EXPECTED_COUNT: 10

-- Q187: REPLACE
SELECT customer_id, REPLACE(email, '@', ' at ') AS safe_email FROM customers LIMIT 10;
-- EXPECTED_COUNT: 10
"#
        .to_string()
    }

    fn gen_math_functions(&self) -> String {
        r#"-- 20_math_functions.sql - Math functions
-- Generated by tsq

-- Q190: ABS
SELECT * FROM orders WHERE ABS(discount_percent - 15) <= 5 LIMIT 50;
-- EXPECTED_COUNT varies

-- Q191: ROUND
SELECT product_id, price, ROUND(price) AS rounded_price FROM products LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q192: CEIL
SELECT product_id, price, CEIL(price) AS ceiling FROM products LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q193: FLOOR
SELECT product_id, price, FLOOR(price) AS floor FROM products LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q194: Arithmetic expressions
SELECT product_id, price, cost, (price - cost) AS profit FROM products LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q195: Percentage calculation
SELECT product_id, price, cost, (price - cost) / price * 100 AS margin_pct FROM products WHERE price > 0 LIMIT 20;
-- EXPECTED_COUNT: 20
"#.to_string()
    }

    fn gen_case_coalesce(&self) -> String {
        r#"-- 21_case_coalesce.sql - CASE, COALESCE, NULLIF
-- Generated by tsq

-- Q200: Simple CASE
SELECT customer_id, credit_score,
       CASE
           WHEN credit_score >= 800 THEN 'Excellent'
           WHEN credit_score >= 700 THEN 'Good'
           WHEN credit_score >= 600 THEN 'Fair'
           ELSE 'Poor'
       END AS credit_tier
FROM customers LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q201: CASE in WHERE
SELECT * FROM customers
WHERE CASE WHEN credit_score > 700 THEN 1 ELSE 0 END = 1
LIMIT 50;
-- EXPECTED_COUNT: 50

-- Q202: COALESCE
SELECT customer_id, COALESCE(notes, 'No notes') AS notes_display
FROM customers LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q203: NULLIF
SELECT product_id, NULLIF(quantity_in_stock, 0) AS stock_or_null
FROM products LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q204: Nested CASE
SELECT order_id, total_amount,
       CASE
           WHEN total_amount > 1000 THEN 'Premium'
           WHEN total_amount > 500 THEN 'Standard'
           WHEN total_amount > 100 THEN 'Basic'
           ELSE 'Micro'
       END AS order_tier
FROM orders LIMIT 20;
-- EXPECTED_COUNT: 20
"#
        .to_string()
    }

    fn gen_mutation_insert(&self) -> String {
        r#"-- 22_mutation_insert.sql - INSERT statements
-- Generated by tsq
-- NOTE: Run with --write flag to persist changes

-- Q210: INSERT single row
INSERT INTO customers (customer_id, name, email, city, state, country, signup_date, is_active, credit_score)
VALUES (999999, 'Test User', 'test@example.com', 'Test City', 'TS', 'USA', '2026-01-01', 1, 750);

-- Q211: Verify insert
SELECT * FROM customers WHERE customer_id = 999999;
-- EXPECTED_COUNT: 1

-- Q212: INSERT with expression values
INSERT INTO products (product_id, name, category, subcategory, price, cost, quantity_in_stock, is_discontinued, created_date)
VALUES (999999, 'Test Product', 'Electronics', 'Phones', 99.99, 49.99, 100, 0, '2026-01-01');

-- Q213: Verify product insert
SELECT * FROM products WHERE product_id = 999999;
-- EXPECTED_COUNT: 1
"#.to_string()
    }

    fn gen_mutation_update(&self) -> String {
        r#"-- 23_mutation_update.sql - UPDATE statements
-- Generated by tsq
-- NOTE: Run with --write flag to persist changes

-- Q220: Count before update
SELECT COUNT(*) AS before_count FROM customers WHERE is_active = 0 AND credit_score < 400;
-- Record this count

-- Q221: UPDATE with WHERE
UPDATE customers SET is_active = 0 WHERE credit_score < 400;

-- Q222: Verify update
SELECT COUNT(*) AS after_count FROM customers WHERE is_active = 0 AND credit_score < 400;
-- EXPECTED: count should match credit_below_400

-- Q223: UPDATE products
UPDATE products SET quantity_in_stock = quantity_in_stock + 10 WHERE is_discontinued = 0;

-- Q224: Verify product update
SELECT COUNT(*) FROM products WHERE is_discontinued = 0;
-- EXPECTED_COUNT: 1
"#
        .to_string()
    }

    fn gen_mutation_delete(&self) -> String {
        let v = &self.verification;
        let cancelled_count = v.status_counts.get("cancelled").copied().unwrap_or(0);

        format!(
            r#"-- 24_mutation_delete.sql - DELETE statements
-- Generated by tsq
-- NOTE: Run with --write flag to persist changes

-- Q230: Count before delete
SELECT COUNT(*) AS before_delete FROM orders WHERE status = 'cancelled';
-- EXPECTED_VALUE: approximately {cancelled_count}

-- Q231: DELETE with WHERE
DELETE FROM orders WHERE status = 'cancelled';

-- Q232: Verify delete
SELECT COUNT(*) AS after_delete FROM orders WHERE status = 'cancelled';
-- EXPECTED_VALUE: 0

-- Q233: DELETE from reviews (low rating)
SELECT COUNT(*) FROM reviews WHERE rating = 1;
-- Record count before

-- Q234: Execute delete
DELETE FROM reviews WHERE rating = 1;

-- Q235: Verify
SELECT COUNT(*) FROM reviews WHERE rating = 1;
-- EXPECTED_VALUE: 0
"#,
            cancelled_count = cancelled_count
        )
    }

    fn gen_complex_combined(&self) -> String {
        r#"-- 25_complex_combined.sql - Complex combined queries
-- Generated by tsq

-- Q240: Multi-table aggregate with GROUP BY and ORDER BY
SELECT c.city, c.state,
       COUNT(DISTINCT o.order_id) AS order_count,
       SUM(o.total_amount) AS total_revenue,
       AVG(o.total_amount) AS avg_order
FROM customers c, orders o
WHERE c.customer_id = o.customer_id AND o.status = 'delivered'
GROUP BY c.city, c.state
ORDER BY total_revenue DESC
LIMIT 20;
-- EXPECTED_COUNT: 20

-- Q241: Subquery with aggregate
SELECT category, AVG(price) AS avg_price
FROM products
WHERE price > (SELECT AVG(price) FROM products)
GROUP BY category
ORDER BY avg_price DESC;
-- EXPECTED_COUNT varies

-- Q242: Window function with JOIN
SELECT c.name, o.order_id, o.total_amount,
       ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY o.total_amount DESC) AS order_rank
FROM customers c, orders o
WHERE c.customer_id = o.customer_id
LIMIT 100;
-- EXPECTED_COUNT: 100

-- Q243: Complex filter with multiple conditions
SELECT c.customer_id, c.name, c.credit_score, COUNT(o.order_id) AS orders
FROM customers c, orders o
WHERE c.customer_id = o.customer_id
  AND c.is_active = 1
  AND c.credit_score > 650
  AND o.status IN ('delivered', 'shipped')
  AND o.total_amount > 100
GROUP BY c.customer_id, c.name, c.credit_score
HAVING COUNT(o.order_id) >= 2
ORDER BY orders DESC
LIMIT 30;
-- EXPECTED_COUNT: 30

-- Q244: UNION with aggregates
SELECT 'High Value' AS segment, COUNT(*) AS cnt FROM orders WHERE total_amount > 1000
UNION ALL
SELECT 'Medium Value' AS segment, COUNT(*) AS cnt FROM orders WHERE total_amount BETWEEN 100 AND 1000
UNION ALL
SELECT 'Low Value' AS segment, COUNT(*) AS cnt FROM orders WHERE total_amount < 100;
-- EXPECTED_COUNT: 3

-- Q245: Four-table join with aggregates
SELECT p.category,
       COUNT(DISTINCT c.customer_id) AS unique_customers,
       COUNT(DISTINCT o.order_id) AS order_count,
       SUM(oi.line_total) AS total_sales
FROM customers c, orders o, order_items oi, products p
WHERE c.customer_id = o.customer_id
  AND o.order_id = oi.order_id
  AND oi.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;
-- EXPECTED_COUNT varies by categories
"#.to_string()
    }
}

// ============================================================================
// Verification Generator
// ============================================================================

struct VerificationGenerator<'a> {
    verification: &'a VerificationData,
    seed: u64,
}

impl<'a> VerificationGenerator<'a> {
    fn new(verification: &'a VerificationData, seed: u64) -> Self {
        Self { verification, seed }
    }

    fn generate_all(&self, base_path: &Path) -> Result<()> {
        self.write_expected_counts(&base_path.join("verify/expected_counts.txt"))?;
        self.write_verification_script(&base_path.join("verify/run_verification.sh"))?;
        Ok(())
    }

    fn write_expected_counts(&self, path: &Path) -> Result<()> {
        let v = &self.verification;
        let content = format!(
            r#"# Expected counts for verification
# Generated by tsq with seed: {}

# Table row counts
customers: {}
products: {}
orders: {}
order_items: {}
reviews: {}

# Distribution counts
customers_with_notes: {}
reviews_with_body: {}
active_customers: {}
discontinued_products: {}
customers_with_orders: {}

# Credit score distribution
credit_below_400: {}
credit_600_to_750: {}
credit_above_700: {}
credit_above_800: {}
min_credit_score: {}
max_credit_score: {}

# Price distribution
price_below_100: {}

# Status distribution
{}

# City distribution
{}
"#,
            self.seed,
            v.customer_count,
            v.product_count,
            v.order_count,
            v.order_item_count,
            v.review_count,
            v.customers_with_notes,
            v.reviews_with_body,
            v.active_customers,
            v.discontinued_products,
            v.customers_with_orders,
            v.credit_below_400,
            v.credit_600_to_750,
            v.credit_above_700,
            v.credit_above_800,
            v.min_credit_score,
            v.max_credit_score,
            v.price_below_100,
            v.status_counts
                .iter()
                .map(|(k, c)| format!("status_{}: {}", k, c))
                .collect::<Vec<_>>()
                .join("\n"),
            v.city_counts
                .iter()
                .take(5)
                .map(|(k, c)| format!("city_{}: {}", k.replace(' ', "_"), c))
                .collect::<Vec<_>>()
                .join("\n"),
        );

        let mut file = File::create(path)?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }

    fn write_verification_script(&self, path: &Path) -> Result<()> {
        let v = &self.verification;
        let content = format!(
            r#"#!/bin/bash
# Verification script for tsq-generated data
# Seed: {}
# Run this script from the output directory
#
# Usage: SQAWK=/path/to/sqawk ./run_verification.sh
#   or:  SQAWK="cargo run --bin sqawk --" ./run_verification.sh

SQAWK="${{SQAWK:-sqawk}}"
DATA_DIR="./data"
PASS=0
FAIL=0

echo "=== TSQ Verification Script ==="
echo "Seed: {}"
echo "Data directory: $DATA_DIR"
echo "Using sqawk: $SQAWK"
echo ""

# Test if sqawk is available
if ! $SQAWK -s "SELECT 1" /dev/null 2>/dev/null; then
    echo "ERROR: sqawk not found or not working"
    echo "Set SQAWK environment variable to the path of sqawk binary"
    echo "  e.g., SQAWK=/path/to/sqawk ./run_verification.sh"
    echo "  or:   SQAWK='cargo run --bin sqawk --' ./run_verification.sh"
    exit 1
fi

check_count() {{
    local desc="$1"
    local expected="$2"
    local sql="$3"
    local files="$4"

    # Run sqawk and get the last line (data row, skipping header)
    result=$($SQAWK -s "$sql" $files 2>/dev/null | tail -n 1)

    if [ "$result" = "$expected" ]; then
        echo "[PASS] $desc: $result"
        ((PASS++)) || true
    else
        echo "[FAIL] $desc: expected $expected, got '$result'"
        ((FAIL++)) || true
    fi
}}

echo "--- Row Count Verification ---"
check_count "Customer count" "{}" "SELECT COUNT(*) FROM customers" "$DATA_DIR/customers.csv"
check_count "Product count" "{}" "SELECT COUNT(*) FROM products" "$DATA_DIR/products.csv"
check_count "Order count" "{}" "SELECT COUNT(*) FROM orders" "$DATA_DIR/orders.csv"
check_count "Order item count" "{}" "SELECT COUNT(*) FROM order_items" "$DATA_DIR/order_items.csv"
check_count "Review count" "{}" "SELECT COUNT(*) FROM reviews" "$DATA_DIR/reviews.csv"

echo ""
echo "--- Distribution Verification ---"
check_count "Customers with notes" "{}" "SELECT COUNT(notes) FROM customers" "$DATA_DIR/customers.csv"
check_count "Active customers" "{}" "SELECT COUNT(*) FROM customers WHERE is_active = 1" "$DATA_DIR/customers.csv"
check_count "Credit > 700" "{}" "SELECT COUNT(*) FROM customers WHERE credit_score > 700" "$DATA_DIR/customers.csv"

echo ""
echo "=== Results ==="
echo "Passed: $PASS"
echo "Failed: $FAIL"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
echo "All tests passed!"
"#,
            self.seed,
            self.seed,
            v.customer_count,
            v.product_count,
            v.order_count,
            v.order_item_count,
            v.review_count,
            v.customers_with_notes,
            v.active_customers,
            v.credit_above_700,
        );

        let mut file = File::create(path)?;
        file.write_all(content.as_bytes())?;

        // Make executable on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(path)?.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(path, perms)?;
        }

        Ok(())
    }
}

// ============================================================================
// Metadata Writer
// ============================================================================

fn write_metadata(base_path: &Path, seed: u64, rows: usize, v: &VerificationData) -> Result<()> {
    let content = format!(
        r#"{{
  "seed": {},
  "base_rows": {},
  "row_counts": {{
    "customers": {},
    "products": {},
    "orders": {},
    "order_items": {},
    "reviews": {}
  }},
  "tsq_version": "0.1.0"
}}
"#,
        seed,
        rows,
        v.customer_count,
        v.product_count,
        v.order_count,
        v.order_item_count,
        v.review_count
    );

    let mut file = File::create(base_path.join("metadata.json"))?;
    file.write_all(content.as_bytes())?;
    Ok(())
}

// ============================================================================
// Main Entry Point
// ============================================================================

fn main() -> Result<()> {
    let args = Args::parse();

    // Determine seed
    let seed = args.seed.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    });

    println!("TSQ - Test SQL Query Generator for sqawk");
    println!("=========================================");
    println!("Seed: {}", seed);
    println!("Rows: {} (base customer count)", args.rows);
    println!("Output: {}", args.output_dir);
    println!();

    // Create output directories
    let base_path = Path::new(&args.output_dir);
    fs::create_dir_all(base_path.join("data")).context("Failed to create data directory")?;
    fs::create_dir_all(base_path.join("queries")).context("Failed to create queries directory")?;
    fs::create_dir_all(base_path.join("verify")).context("Failed to create verify directory")?;

    // Generate data
    println!("Generating data...");
    let mut generator = DataGenerator::new(seed, args.rows, args.verbose);
    generator.generate_all(base_path)?;
    println!();

    // Generate queries
    println!("Generating queries...");
    let query_gen = QueryGenerator::new(&generator.verification);
    query_gen.generate_all(base_path)?;
    println!("  Created 25 query files");
    println!();

    // Generate verification
    println!("Generating verification scripts...");
    let verify_gen = VerificationGenerator::new(&generator.verification, seed);
    verify_gen.generate_all(base_path)?;
    println!("  Created expected_counts.txt");
    println!("  Created run_verification.sh");
    println!();

    // Write metadata
    write_metadata(base_path, seed, args.rows, &generator.verification)?;
    println!("  Created metadata.json");
    println!();

    // Summary
    let v = &generator.verification;
    println!("Generation complete!");
    println!("-----------------------------------------");
    println!("Tables generated:");
    println!("  customers:    {:>10} rows", v.customer_count);
    println!("  products:     {:>10} rows", v.product_count);
    println!("  orders:       {:>10} rows", v.order_count);
    println!("  order_items:  {:>10} rows", v.order_item_count);
    println!("  reviews:      {:>10} rows", v.review_count);
    println!("-----------------------------------------");
    println!(
        "Total rows:     {:>10}",
        v.customer_count + v.product_count + v.order_count + v.order_item_count + v.review_count
    );
    println!();
    println!("To run sqawk on generated data:");
    println!(
        "  sqawk -s \"SELECT * FROM customers LIMIT 10\" {}/data/customers.csv",
        args.output_dir
    );
    println!();
    println!("To run verification:");
    println!(
        "  cd {} && bash verify/run_verification.sh",
        args.output_dir
    );

    Ok(())
}

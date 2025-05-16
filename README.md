# Scala Functional Programming Discount Rule Engine

A **functional programming** implementation of a discount rule engine for a retail store, built in **Scala**. This engine processes orders from CSV files, applies multiple discount rules, stores results in a **PostgreSQL** database, and logs operations to a file.

---

## Project Overview

This discount rule engine processes retail transactions and automatically calculates discounts based on multiple qualifying rules. The engine is implemented using **functional programming principles** in Scala, ensuring **pure functions** and **immutable** data structures.

---

## Technologies Used

* **Language:** Scala 2.13+ (Functional Programming)
* **Database:** PostgreSQL 12+
* **CSV:** Input/output via standard Scala libraries
* **JDBC:** For PostgreSQL integration
* **Logging:** Custom logging format to file

---

## Discount Rules

The engine implements the following discount rules:

1. **Product Expiry Discount**
   - Products expiring within 30 days
   - Discount increases by 1% for each day closer to expiry
   - Example: 29 days remaining = 1%, 28 days = 2%, etc.

2. **Product Type Discount**
   - Cheese products: 10% discount
   - Wine products: 5% discount

3. **Special Date Discount**
   - 50% discount on all products sold on March 23rd

4. **Bulk Purchase Discount**
   - 6-9 units: 5% discount
   - 10-14 units: 7% discount
   - 15+ units: 10% discount

5. **App Purchase Discount**
   - Discount based on quantity rounded up to nearest multiple of 5
   - Example: 1-5 units = 5%, 6-10 units = 10%, etc.

6. **Payment Method Discount**
   - 5% discount for Visa card payments

---

## Database Schema

Results are stored in the following table:

```sql
CREATE TABLE calculated_discounts (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    product_name VARCHAR(255),
    expiry_date DATE,
    quantity INT,
    unit_price DECIMAL(10, 2),
    channel VARCHAR(50),
    payment_method VARCHAR(50),
    discount_percentage DECIMAL(5, 2),
    final_price DECIMAL(10, 2)
);
```

---

## Setup Instructions

### Prerequisites

* Java JDK 8+
* Scala 2.13+
* PostgreSQL 12+
* sbt (Scala build tool)

### Database Setup

```sql
CREATE DATABASE discount_db;
CREATE USER discount_user WITH PASSWORD 'discount_pass';
GRANT ALL PRIVILEGES ON DATABASE discount_db TO discount_user;
```

The table `calculated_discounts` will be auto-created if it doesn’t exist.

---

## How to Run the Project

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/Scala_FP_Discount_Rule_Engine.git
cd Scala_FP_Discount_Rule_Engine
```

### 2. Add Input File

Place your transaction CSV at:

```
src/main/resources/TRX1000.csv
```

Ensure the file contains these columns (in order):

```
timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method
```

### 3. Build and Run

```bash
sbt compile
sbt run
```

---

## Output

* `src/main/resources/calculated_discount.csv`: Final results with discounts applied
* `src/main/resources/rules_engine.log`: Logs of all processing steps
* `PostgreSQL`: Populated `calculated_discounts` table in `discount_db`

---

## Project Structure

```
src/
├── main/
│   ├── scala/
│   │   └── Project/
│   │       └── Calculation_Rules.scala     # Main rule engine logic
│   └── resources/
│       ├── TRX1000.csv                     # Input file
│       ├── calculated_discount.csv         # Output file
│       └── rules_engine.log                # Log file

```
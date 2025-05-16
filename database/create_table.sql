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
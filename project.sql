CREATE DATABASE IF NOT EXISTS project;
use project;

-- Create the Customer Dimension Table
CREATE TABLE Customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    gender VARCHAR(50)
);

-- Create the Product Dimension Table
CREATE TABLE Product (
    productID INT PRIMARY KEY,
    productName VARCHAR(255),
    productPrice DECIMAL(10, 2),
    supplierID INT,
    supplierName VARCHAR(255),
    storeID INT,
    storeName VARCHAR(255)
);

-- Create the Time Dimension Table
CREATE TABLE TimeDimension (
    time_id INT PRIMARY KEY,
    order_date DATETIME,
    day_of_week VARCHAR(50),
    month INT,
    year INT,
    quarter INT
);

-- Create the Fact Table (Transaction Table)
CREATE TABLE TransactionFact (
    order_id INT PRIMARY KEY,
    order_date DATETIME,
    productID INT,
    quantity_ordered INT,
    customer_id INT,
    time_id INT,
    FOREIGN KEY (productID) REFERENCES Product(productID),
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id),
    FOREIGN KEY (time_id) REFERENCES TimeDimension(time_id)
);

-- Load data into the Customer table from CSV
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/customers.csv'
INTO TABLE Customer
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, customer_name, gender);

-- Load data into the Product table from CSV
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/products.csv'
INTO TABLE Product
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(productID, productName, productPrice, supplierID, supplierName, storeID, storeName);

-- Load data into the TimeDimension table from CSV
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/time_dimension.csv'
INTO TABLE TimeDimension
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(time_id, order_date, day_of_week, month, year, quarter);

-- Load data into the TransactionFact table from CSV
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/transactions.csv'
INTO TABLE TransactionFact
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, order_date, productID, quantity_ordered, customer_id, time_id);

-- Verify data loading
SELECT * FROM Customer;
SELECT * FROM Product;
SELECT * FROM TimeDimension;
SELECT * FROM TransactionFact;



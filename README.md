# Building-and-Analysing-a-Near-Real-Time-Data-Warehouse
Design, implement, and analyse a near-real-time Data Warehouse (DW) prototype for METRO shopping store in Pakistan.
# **Data Warehouse Project: ETL and Analysis Using Java and SQL**

## **Overview**

This project demonstrates the creation and utilization of a data warehouse for analyzing transactional data from a relational database. The workflow involves extracting data from a **source database**, transforming it using **Java-based ETL (Extract, Transform, Load) processes**, and storing it in a **target data warehouse** for querying and analysis.

The primary components of this project include:
1. **Database Design:** Relational tables in a source database (`project`) and an enriched fact table in a data warehouse (`dw`).
2. **ETL Process:** A Java program to integrate and transform data from the source database into the target warehouse.
3. **Data Analysis:** SQL queries for business insights.


## Table Definitions

### Project Database
```sql
CREATE DATABASE IF NOT EXISTS project;
USE project;

CREATE TABLE Customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    gender VARCHAR(50)
);

CREATE TABLE Product (
    productID INT PRIMARY KEY,
    productName VARCHAR(255),
    productPrice DECIMAL(10, 2),
    supplierID INT,
    supplierName VARCHAR(255),
    storeID INT,
    storeName VARCHAR(255)
);

CREATE TABLE TimeDimension (
    time_id INT PRIMARY KEY,
    order_date DATETIME,
    day_of_week VARCHAR(50),
    month INT,
    year INT,
    quarter INT
);

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
```

### Data Warehouse
```sql
CREATE DATABASE IF NOT EXISTS dw;
USE dw;

CREATE TABLE EnrichedTransactionFact (
    enriched_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    customer_id INT NOT NULL,
    productID INT NOT NULL,
    time_id INT NOT NULL,
    quantity_ordered INT NOT NULL,
    total_sale DECIMAL(15, 2) NOT NULL,
    productPrice DECIMAL(10, 2),
    productName VARCHAR(255),
    supplierID INT,
    supplierName VARCHAR(255),
    storeID INT,
    storeName VARCHAR(255),
    customer_name VARCHAR(255),
    gender VARCHAR(50),
    order_date DATETIME,
    day_of_week VARCHAR(50),
    month INT,
    year INT,
    quarter INT,
    FOREIGN KEY (customer_id) REFERENCES project.Customer(customer_id),
    FOREIGN KEY (productID) REFERENCES project.Product(productID)
);
```

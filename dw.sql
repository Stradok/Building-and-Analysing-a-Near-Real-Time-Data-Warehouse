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
    FOREIGN KEY (productID) REFERENCES project.Product(productID),
    FOREIGN KEY (time_id) REFERENCES project.TimeDimension(time_id)
);


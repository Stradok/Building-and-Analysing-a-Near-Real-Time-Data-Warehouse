# Building-and-Analysing-a-Near-Real-Time-Data-Warehouse
Design, implement, and analyse a near-real-time Data Warehouse (DW) prototype for METRO shopping store in Pakistan.
# **Data Warehouse Project: ETL and Analysis Using Java and SQL**

## **Overview**

This project demonstrates the creation and utilization of a data warehouse for analyzing transactional data from a relational database. The workflow involves extracting data from a **source database**, transforming it using **Java-based ETL (Extract, Transform, Load) processes**, and storing it in a **target data warehouse** for querying and analysis.

The primary components of this project include:
1. **Database Design:** Relational tables in a source database (`project`) and an enriched fact table in a data warehouse (`dw`).
2. **ETL Process:** A Java program to integrate and transform data from the source database into the target warehouse.
3. **Data Analysis:** SQL queries for business insights.

---

## **Project Structure**

### **1. Source Database Schema (`project`)**

The source database contains the following tables:

- **`Customer`**:
  - Stores customer information such as name and gender.
  - Primary Key: `customer_id`.

- **`Product`**:
  - Contains product details, including price, supplier, and store information.
  - Primary Key: `productID`.

- **`TimeDimension`**:
  - Captures temporal details for transactions.
  - Primary Key: `time_id`.

- **`TransactionFact`**:
  - A fact table storing transactional data, including the quantity of products ordered and related IDs.
  - Primary Key: `order_id`.

### **2. Data Warehouse Schema (`dw`)**

The enriched data warehouse table consolidates source data into a single table (`EnrichedTransactionFact`) for efficient querying and analysis. Key features include:
- Transactional details (e.g., `order_id`, `quantity_ordered`, `total_sale`).
- Dimensional attributes (e.g., `productName`, `customer_name`, `supplierName`).
- Temporal fields (e.g., `order_date`, `day_of_week`, `month`, `quarter`, `year`).

---

## **ETL Process**

The ETL (Extract, Transform, Load) workflow is implemented in **Java**. Key components include:

### **Java Program (ETL Pipeline)**
- **Extraction:** Data is streamed from the source database using JDBC.
- **Transformation:** Data from multiple tables is joined and enriched (e.g., calculating total sales from `quantity_ordered` and `productPrice`).
- **Loading:** Transformed data is inserted into the `EnrichedTransactionFact` table in the data warehouse.

### **Java Implementation Details**
- **Technologies:**
  - Java JDK 22
  - Eclipse IDE 9.0.0
  - MySQL JDBC Connector
- **Classes and Methods:**
  - **Transaction, Product, Customer, Time:** Classes to hold extracted data.
  - **StreamThread:** Reads transactional data.
  - **MasterDataThread:** Reads dimensional data (Product, Customer, Time).
  - **populateDataWarehouse:** Combines data and inserts enriched records into the data warehouse.

---

## **Data Loading Process**

The source tables are populated using the `LOAD DATA INFILE` command from CSV files. Sample loading scripts include:
```sql
LOAD DATA INFILE 'path/to/customers.csv'
INTO TABLE Customer
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, customer_name, gender);

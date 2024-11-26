//start seeing from the main below to better understand the code flow

package testing;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CombinedETL {

    // Database connection details
    static String sourceDbName = "project";
    static String sourceDbUsername = "root";
    static String sourceDbPassword = "amman121";

    static String targetDbName = "dw";
    static String targetDbUsername = "root";
    static String targetDbPassword = "amman121";

    // Data structures for streaming and master data
    //custom classes to represent tge datga entries in the database for fetching 
    static class Transaction 
    {
        int orderId;
        int productId;
        int customerId;
        int timeId;
        int quantityOrdered;

        Transaction(int orderId, int productId, int customerId, int timeId, int quantityOrdered)
        {
            this.orderId = orderId;
            this.productId = productId;
            this.customerId = customerId;
            this.timeId = timeId;
            this.quantityOrdered = quantityOrdered;
        }
    }

    
    
    
    //class defind  for products
    
    static class Product 
    {
        int productId;
        String productName;
        double productPrice;
        int supplierID;
        String supplierName;
        int storeID;
        String storeName;

        Product(int productId, String productName, double productPrice, int supplierID, String supplierName, int storeID, String storeName)
        {
            this.productId = productId;
            this.productName = productName;
            this.productPrice = productPrice;
            this.supplierID = supplierID;
            this.supplierName = supplierName;
            this.storeID = storeID;
            this.storeName = storeName;
        }
    }

    
    
    //class defined for customers
    static class Customer
    {
        int customerId;
        String customerName;
        String gender;

        Customer(int customerId, String customerName, String gender)
        {
            this.customerId = customerId;
            this.customerName = customerName;
            this.gender = gender;
        }
    }
    
    //class defined for times

    static class Time 
    {
        int timeId;
        Date orderDate;
        String dayOfWeek;
        int month;
        int year;
        int quarter;

        Time(int timeId, Date orderDate, String dayOfWeek, int month, int year, int quarter)
        {
            this.timeId = timeId;
            this.orderDate = orderDate;
            this.dayOfWeek = dayOfWeek;
            this.month = month;
            this.year = year;
            this.quarter = quarter;
        }
    }

    
    
    //the class defined for enriched transanction this table is in the new datawarehouse designed to store all the data and transformed data
    static class EnrichedTransaction 
    {
        int orderId;
        int customerId;
        int productId;
        int timeId;
        String customerName;
        String gender;
        String productName;
        double productPrice;
        int quantityOrdered;
        double totalSale;
        int supplierID;
        String supplierName;
        int storeID;
        String storeName;
        Date orderDate;
        String dayOfWeek;
        int month;
        int year;
        int quarter;

        EnrichedTransaction(Transaction transaction, Product product, Customer customer, Time time) 
        {
            this.orderId = transaction.orderId;
            this.customerId = transaction.customerId;
            this.productId = transaction.productId;
            this.timeId = transaction.timeId;
            this.customerName = customer.customerName;
            this.gender = customer.gender;
            this.productName = product.productName;
            this.productPrice = product.productPrice;
            this.quantityOrdered = transaction.quantityOrdered;
            this.totalSale = this.quantityOrdered * this.productPrice;
            this.supplierID = product.supplierID;
            this.supplierName = product.supplierName;
            this.storeID = product.storeID;
            this.storeName = product.storeName;
            this.orderDate = time.orderDate;
            this.dayOfWeek = time.dayOfWeek;
            this.month = time.month;
            this.year = time.year;
            this.quarter = time.quarter;
        }
    }

    
    //queues made so store the streaming data incomming for the buffer
    
    
    static ConcurrentLinkedQueue<Transaction> transactionBuffer = new ConcurrentLinkedQueue<>();
    static ConcurrentLinkedQueue<Product> productBuffer = new ConcurrentLinkedQueue<>();
    static ConcurrentLinkedQueue<Customer> customerBuffer = new ConcurrentLinkedQueue<>();
    static ConcurrentLinkedQueue<Time> timeBuffer = new ConcurrentLinkedQueue<>();

    
    //loading transanction data into the buffer 
    
    static class StreamThread extends Thread 
    {
        public void run() 
        {
            try (Connection connection = DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1:3306/" + sourceDbName, sourceDbUsername, sourceDbPassword)) {

                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM TransactionFact");

                while (resultSet.next())
                {
                    Transaction transaction = new Transaction(
                            resultSet.getInt("order_id"),
                            resultSet.getInt("productID"),
                            resultSet.getInt("customer_id"),
                            resultSet.getInt("time_id"),
                            resultSet.getInt("quantity_ordered")
                    );
                    transactionBuffer.add(transaction);
                }

            } catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
    }
    
    
    //extracting data from product custmoer and time

    static class MasterDataThread extends Thread
    {
        public void run()
        {
            try (Connection connection = DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1:3306/" + sourceDbName, sourceDbUsername, sourceDbPassword))
            {

                Statement statement = connection.createStatement();

                // Load product data
                ResultSet productResultSet = statement.executeQuery("SELECT * FROM Product");
                while (productResultSet.next()) {
                    Product product = new Product(
                            productResultSet.getInt("productID"),
                            productResultSet.getString("productName"),
                            productResultSet.getDouble("productPrice"),
                            productResultSet.getInt("supplierID"),
                            productResultSet.getString("supplierName"),
                            productResultSet.getInt("storeID"),
                            productResultSet.getString("storeName")
                    );
                    productBuffer.add(product);
                }

                // Load customer data
                ResultSet customerResultSet = statement.executeQuery("SELECT * FROM Customer");
                while (customerResultSet.next()) {
                    Customer customer = new Customer(
                            customerResultSet.getInt("customer_id"),
                            customerResultSet.getString("customer_name"),
                            customerResultSet.getString("gender")
                    );
                    customerBuffer.add(customer);
                }

                // Load time data
                ResultSet timeResultSet = statement.executeQuery("SELECT * FROM TimeDimension");
                while (timeResultSet.next()) {
                    Time time = new Time(
                            timeResultSet.getInt("time_id"),
                            timeResultSet.getDate("order_date"),
                            timeResultSet.getString("day_of_week"),
                            timeResultSet.getInt("month"),
                            timeResultSet.getInt("year"),
                            timeResultSet.getInt("quarter")
                    );
                    timeBuffer.add(time);
                }

            } catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
    }
 
    
    
    
    //storing the datawarehouise 
    static void populateDataWarehouse(List<EnrichedTransaction> enrichedTransactions)
    {
        try (Connection connection = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:3306/" + targetDbName, targetDbUsername, targetDbPassword))
        {

            String sqlInsert = "INSERT INTO EnrichedTransactionFact (order_id, customer_id, productID, time_id, " +
                    "quantity_ordered, total_sale, productPrice, productName, supplierID, supplierName, storeID, storeName, " +
                    "customer_name, gender, order_date, day_of_week, month, year, quarter) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement preparedStatement = connection.prepareStatement(sqlInsert)) 
            {
                for (EnrichedTransaction transaction : enrichedTransactions) {
                    preparedStatement.setInt(1, transaction.orderId);
                    preparedStatement.setInt(2, transaction.customerId);
                    preparedStatement.setInt(3, transaction.productId);
                    preparedStatement.setInt(4, transaction.timeId);
                    preparedStatement.setInt(5, transaction.quantityOrdered);
                    preparedStatement.setDouble(6, transaction.totalSale);
                    preparedStatement.setDouble(7, transaction.productPrice);
                    preparedStatement.setString(8, transaction.productName);
                    preparedStatement.setInt(9, transaction.supplierID);
                    preparedStatement.setString(10, transaction.supplierName);
                    preparedStatement.setInt(11, transaction.storeID);
                    preparedStatement.setString(12, transaction.storeName);
                    preparedStatement.setString(13, transaction.customerName);
                    preparedStatement.setString(14, transaction.gender);
                    preparedStatement.setDate(15, transaction.orderDate);
                    preparedStatement.setString(16, transaction.dayOfWeek);
                    preparedStatement.setInt(17, transaction.month);
                    preparedStatement.setInt(18, transaction.year);
                    preparedStatement.setInt(19, transaction.quarter);

                    preparedStatement.executeUpdate();
                }
            }

        } catch (SQLException e) 
        {
            e.printStackTrace();
        }
    }
    
    
    
    
    //Wait for both threads to complete their tasks.
    //Initialize an empty list for EnrichedTransaction objects.
    //Iterate through transactionBuffer to match and enrich data with buffers for Product, Customer, and Time.
    //Add successfully enriched transactions to the list.
    //Load enriched transactions into the data warehouse.
    

    public static void main(String[] args) 
    {
    	//Initialize and start StreamThread and MasterDataThread to extract data.
        Thread streamThread = new StreamThread();
        Thread masterDataThread = new MasterDataThread();

        streamThread.start();
        masterDataThread.start();

        try 
        {
        	 // Wait for both threads to finish their execution before proceeding
            streamThread.join();
            masterDataThread.join();
        } 
        catch (InterruptedException e) 
        {
            e.printStackTrace();
        }

        // Create an empty list to store enriched transactions
        List<EnrichedTransaction> enrichedTransactions = new ArrayList<>();

        
        // Iterate through the transaction buffer to enrich each transaction with product, customer, and time data
        for (Transaction transaction : transactionBuffer)
        {
            Product product = productBuffer.stream()
                    .filter(p -> p.productId == transaction.productId)
                    .findFirst().orElse(null);

            Customer customer = customerBuffer.stream()	
                    .filter(c -> c.customerId == transaction.customerId)
                    .findFirst().orElse(null);

            Time time = timeBuffer.stream()
                    .filter(t -> t.timeId == transaction.timeId)
                    .findFirst().orElse(null);
            // If all related data (product, customer, time) is found, create an enriched transaction
            if (product != null && customer != null && time != null)
            {
            	// Create an enriched transaction object by combining transaction with product, customer, and time
                enrichedTransactions.add(new EnrichedTransaction(transaction, product, customer, time));
            }
        }

        // Once all transactions are enriched, load them into the data warehouse
        populateDataWarehouse(enrichedTransactions);
    }
}

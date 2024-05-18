import mysql.connector
import os
from dotenv import load_dotenv

def create_db_tables(cursor):
    # Create Customers table
    cursor.execute('''CREATE TABLE IF NOT EXISTS Customers (
                        CustomerID INT PRIMARY KEY,
                        FirstName VARCHAR(255),
                        LastName VARCHAR(255),
                        Email VARCHAR(255),
                        DateOfBirth DATE
                    )''')

    # Create Products table
    cursor.execute('''CREATE TABLE IF NOT EXISTS Products (
                        ProductID INT PRIMARY KEY,
                        ProductName VARCHAR(255),
                        Price DECIMAL(10, 2)
                    )''')

    # Create Orders table
    cursor.execute('''CREATE TABLE IF NOT EXISTS Orders (
                        OrderID INT PRIMARY KEY,
                        CustomerID INT,
                        OrderDate DATE,
                        FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
                    )''')

    # Create OrderItems table
    cursor.execute('''CREATE TABLE IF NOT EXISTS OrderItems (
                        OrderItemID INT PRIMARY KEY,
                        OrderID INT,
                        ProductID INT,
                        Quantity INT,
                        FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
                        FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
                    )''')

def populate_sample_data(cursor):
    # Insert data into Customers table
    customers = [
        (3, 'Alice', 'Johnson', 'alice.johnson@example.com', '1987-04-22'),
        (4, 'Bob', 'Brown', 'bob.brown@example.com', '1992-09-15')
    ]
    cursor.executemany('''INSERT INTO Customers (CustomerID, FirstName, LastName, Email, DateOfBirth)
                          VALUES (%s, %s, %s, %s, %s)''', customers)

    # Insert data into Products table
    products = [
        (1, 'Laptop', 1000),
        (2, 'Smartphone', 600),
        (3, 'Headphones', 100)
    ]
    cursor.executemany('''INSERT INTO Products (ProductID, ProductName, Price)
                          VALUES (%s, %s, %s)''', products)

    # Insert data into Orders table
    orders = [
        (1, 1, '2023-01-10'),
        (2, 2, '2023-01-12')
    ]
    cursor.executemany('''INSERT INTO Orders (OrderID, CustomerID, OrderDate)
                          VALUES (%s, %s, %s)''', orders)

    # Insert data into OrderItems table
    order_items = [
        (1, 1, 1, 1),
        (2, 1, 3, 2),
        (3, 2, 2, 1),
        (4, 2, 3, 1)
    ]
    cursor.executemany('''INSERT INTO OrderItems (OrderItemID, OrderID, ProductID, Quantity)
                          VALUES (%s, %s, %s, %s)''', order_items)

def display_all_customers(cursor):
    cursor.execute("SELECT * FROM Customers")
    for row in cursor.fetchall():
        print(row)

def find_orders_in_jan(cursor):
    cursor.execute("SELECT * FROM Orders WHERE OrderDate BETWEEN '2023-01-01' AND '2023-01-31'")
    for row in cursor.fetchall():
        print(row)

def fetch_order_details(cursor):
    cursor.execute('''SELECT o.OrderID, c.FirstName, c.LastName, c.Email, o.OrderDate
                      FROM Orders o
                      JOIN Customers c ON o.CustomerID = c.CustomerID''')
    for row in cursor.fetchall():
        print(row)

def list_order_products(cursor, order_id):
    cursor.execute('''SELECT p.ProductName, p.Price, oi.Quantity
                      FROM OrderItems oi
                      JOIN Products p ON oi.ProductID = p.ProductID
                      WHERE oi.OrderID = %s''', (order_id,))
    for row in cursor.fetchall():
        print(row)

def calculate_customer_spending(cursor):
    cursor.execute('''SELECT c.CustomerID, c.FirstName, c.LastName, SUM(p.Price * oi.Quantity) AS TotalSpent
                      FROM Customers c
                      JOIN Orders o ON c.CustomerID = o.CustomerID
                      JOIN OrderItems oi ON o.OrderID = oi.OrderID
                      JOIN Products p ON oi.ProductID = p.ProductID
                      GROUP BY c.CustomerID''')
    for row in cursor.fetchall():
        print(row)

def find_top_product(cursor):
    cursor.execute('''SELECT p.ProductName, SUM(oi.Quantity) AS TotalOrdered
                      FROM Products p
                      JOIN OrderItems oi ON p.ProductID = oi.ProductID
                      GROUP BY p.ProductName
                      ORDER BY TotalOrdered DESC
                      LIMIT 1''')
    print(cursor.fetchone())

def get_sales_by_month(cursor):
    cursor.execute('''SELECT MONTH(OrderDate) AS Month, COUNT(*) AS TotalOrders, SUM(p.Price * oi.Quantity) AS TotalSales
                      FROM Orders o
                      JOIN OrderItems oi ON o.OrderID = oi.OrderID
                      JOIN Products p ON oi.ProductID = p.ProductID
                      WHERE YEAR(OrderDate) = 2023
                      GROUP BY MONTH(OrderDate)''')
    for row in cursor.fetchall():
        print(row)

def identify_high_spenders(cursor):
    cursor.execute('''SELECT c.CustomerID, c.FirstName, c.LastName, SUM(p.Price * oi.Quantity) AS TotalSpent
                      FROM Customers c
                      JOIN Orders o ON c.CustomerID = o.CustomerID
                      JOIN OrderItems oi ON o.OrderID = oi.OrderID
                      JOIN Products p ON oi.ProductID = p.ProductID
                      GROUP BY c.CustomerID
                      HAVING TotalSpent > 1000''')
    for row in cursor.fetchall():
        print(row)

try:
    # Load environment variables from .env file
    load_dotenv()
    my_user = os.getenv('user')
    my_password = os.getenv('password')
    my_database = os.getenv('database')

    # Establish connection to MySQL database
    conn = mysql.connector.connect(
        host="localhost",
        user=my_user, # db Id
        password=my_password, # db Password
        database=my_database, # db Name
    )
    cur = conn.cursor()

    # Create tables and insert sample data
    create_db_tables(cur)
    populate_sample_data(cur)
    conn.commit()
    print("Sample data inserted successfully!\n")

    # Execute sample queries
    print("Q1. List all customers:")
    display_all_customers(cur)
    print("\nQ2. Find all orders placed in January 2023:")
    find_orders_in_jan(cur)
    print("\nQ3. Get the details of each order, including the customer name and email:")
    fetch_order_details(cur)
    print("\nQ4. List the products purchased in a specific order (OrderID = 1):")
    list_order_products(cur, 1)
    print("\nQ5. Calculate the total amount spent by each customer:")
    calculate_customer_spending(cur)
    print("\nQ6. Find the most popular product (the one that has been ordered the most):")
    find_top_product(cur)
    print("\nQ7. Get the total number of orders and the total sales amount for each month in 2023:")
    get_sales_by_month(cur)
    print("\nQ8. Find customers who have spent more than $1000:")
    identify_high_spenders(cur)

except mysql.connector.Error as err:
    print("MySQL Error:", err.msg)

finally:
    # Close the cursor and connection
    if 'cur' in locals() and cur is not None:
        cur.close()
    if 'conn' in locals() and conn is not None:
        conn.close()

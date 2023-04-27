```
import random
import time
from datetime import datetime, timedelta
import mysql.connector

cnx = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="spark_streaming"
)

cursor = cnx.cursor()
cursor.execute("SELECT MAX(order_id) FROM orders_data")
result = cursor.fetchone()
if result[0]:
    max_order_id = result[0]
else:
    max_order_id = 0
cursor.close()

while True:

    max_order_id += 1
    
    customer_id = random.randint(101, 120)
    item_id = random.randint(201, 220)
    order_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    item_value = random.randint(10, 30)
    quantity = random.randint(1, 5)
    total_amount = item_value * quantity
    
    query = """
    INSERT INTO orders_data 
    (order_id, customer_id, item_id, order_date, item_value, quantity, total_amount) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor = cnx.cursor()
    cursor.execute(query, (max_order_id, customer_id, item_id, order_date, item_value, quantity, total_amount))
    cnx.commit()
    cursor.close()
    
    time.sleep(10)
    
cnx.close()
```

## RESULTS OF RUNNING THE SCRIPT 

![SQL infinite data injection Script Using Python](C:\Users\Azhar\Desktop\Spark Streaming\SQL_Script_result.png)

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

order_id, customer_id, item_id, order_date, item_value, quantity, total_amount
* 1	101	201	2023-04-27 09:00:00	10	5	50
* 2	102	202	2023-04-27 10:30:00	15	3	45
* 3	103	203	2023-04-27 11:45:00	20	2	40
* 4	104	204	2023-04-27 13:15:00	12	6	72
* 5	105	205	2023-04-27 14:30:00	18	4	72
* 6	106	206	2023-04-27 15:45:00	25	1	25
* 7	111	201	2023-04-27 14:02:18	17	2	34
* 8	106	220	2023-04-27 14:02:28	23	2	46
* 9	107	205	2023-04-27 14:02:38	28	2	56
* 10	112	212	2023-04-27 14:02:48	12	5	60

## Its an infinte script so it will keep inserting data


```
create table orders_data (
	order_id int,
    customer_id int,
    item_id int,
    order_date datetime,
    item_value int,
    quantity int,
    total_amount int
);

```

```
INSERT INTO orders_data (order_id, customer_id, item_id, order_date, item_value, quantity, total_amount)
VALUES (1, 101, 201, '2023-04-27 09:00:00', 10, 5, 50),
		(2, 102, 202, '2023-04-27 10:30:00', 15, 3, 45),
		(3, 103, 203, '2023-04-27 11:45:00', 20, 2, 40),
		(4, 104, 204, '2023-04-27 13:15:00', 12, 6, 72),
		(5, 105, 205, '2023-04-27 14:30:00', 18, 4, 72),
		(6, 106, 206, '2023-04-27 15:45:00', 25, 1, 25)
    
```



# spark-Application 

### The producer.py file:

Imports necessary modules such as argparse, confluent_kafka, json, and random.
Defines a Car class that takes in data attributes such as id, make, model, and year.
Defines a car_to_dict function that converts a Car instance into a Python dictionary.
Defines a generate_random_car function that creates a random Car instance.
Defines a produce function that takes in a Kafka topic name, Kafka configuration settings, and the number of messages to send to the Kafka topic.
In the produce function, a Kafka Producer is created using the provided Kafka configuration settings.
For the given number of messages, a random Car instance is generated and converted to a dictionary, then serialized as JSON and sent to the Kafka topic using the producer instance.
The producer instance is then flushed and closed.

### The consumer.py file:

Imports necessary modules such as argparse, confluent_kafka, and confluent_kafka.schema_registry.
Defines a Car class that takes in a dictionary of car data attributes and converts them into object attributes.
Defines a dict_to_car function that takes in a dictionary and returns an instance of Car.
Defines a main function that takes in a Kafka topic name to consume from.
In the main function, a Confluent Kafka Schema Registry client is created and used to fetch the latest version of the schema for the given Kafka topic.
A Kafka consumer instance is created using the provided Kafka configuration settings, subscribing to the given Kafka topic.
Messages are polled from the Kafka topic, and if a message is received, the message value is deserialized using the fetched JSON schema and dict_to_car function, creating a new Car instance.
The Car instance is then printed to the console.
The consumer instance is closed after the user interrupts the program.

### ABOUT THE PROJECT 

The project is focused on developing a SQL infinite script which is used to insert data into a SQL table named "orders data". Along with that, a Kafka producer and consumer code were also developed to communicate with the SQL table and send/receive data.

The SQL infinite script is a program that executes a sequence of SQL statements in a loop, allowing the insertion of an unlimited amount of data into the "orders data" table. This script is developed using Apache Spark, which is an open-source distributed computing system that is widely used for big data processing. It is designed to work with large-scale data processing by distributing data and computations across a cluster of machines.

The SQL infinite script uses Spark SQL to read the data from a source file and insert it into the "orders data" table. The Spark SQL is a module in Apache Spark that is used for structured data processing. It provides a programming interface to work with structured data using SQL queries, DataFrame, and Dataset API.

To test the SQL infinite script, a Kafka producer code was developed to generate test data. The producer code generates JSON data that contains information about orders, such as order number, product name, quantity, and price. The data is sent to a Kafka topic, which acts as a message queue between the producer and consumer.

The Kafka consumer code was developed to read the data from the Kafka topic and insert it into the "orders data" table using the SQL infinite script. The consumer code is developed using the Kafka Python client, which is a Python library for working with Kafka. It subscribes to the Kafka topic and reads the JSON data using the JSON deserializer. It then passes the data to the SQL infinite script for insertion into the "orders data" table.

Overall, the project is focused on developing a system that can handle large amounts of data and insert it into a SQL table. It uses Apache Spark and Kafka for data processing and communication, respectively. The system can be used in various industries that require real-time data processing and analytics, such as e-commerce, finance, and healthcare.

from pyspark.sql import SparkSession

API_KEY = 'VIMWNFMLY7IRCNTU'
API_SECRET_KEY = 'bWtf9sqCyFnzO+9wgXUylVQCPTDm2owwtgXKjvK7XNfPHV4ecPSRWUGf6HRTsXpY'
BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
ENDPOINT_SCHEMA_URL  = 'https://psrc-6y63j.ap-southeast-2.aws.confluent.cloud'
SCHEMA_REGISTRY_API_KEY = 'W3FWYC527DOMPNE2'
SCHEMA_REGISTRY_API_SECRET = 'JCZcpeOd7GL4KlZlISM35CmCU0/ygAghG1Yzytxt8Zxp/ivLgWqor/u7QfqVs6Oe'

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars", "path/to/kafka-jar1.jar,path/to/kafka-jar2.jar") \
    .getOrCreate()

# Read data from Kafka topic
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092") \
    .option("subscribe", "orders_data") \
    .option("startingOffsets", "earliest") \
    .load()

# Show the result
df.show(truncate=False)

# Stop the SparkSession
spark.stop()

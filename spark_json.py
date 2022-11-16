from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, FloatType
import pyspark.sql.functions as F
import random

# Generating json object
data = []
schema = StructType([
    StructField('restaurant_id', LongType(), False),
    StructField('order', StringType(), False),
    StructField('date_partition', StringType(), False),
])

foods_list = ['hamburguer', 'cheeseburguer', 'pizza', 'hot dog', 'french fries', 'kebab', 'kafta', 'falaffel', 'somosa', 'sushi', 'yakisoba']
drinks_list = ['coca cola', 'water', 'orange juice', 'milk shake', 'energy drink', 'beer']
    
for i in range(10000):    
    order_id     = random.randint(1000, 8000000)
    foods        = random.choices(foods_list, k=random.randint(1,3))
    drinks       = random.choices(drinks_list, k=random.randint(0,2))
    food_prices  = [random.randint(1500, 7599)/100 for j in _foods]
    drink_prices = [random.randint(600, 1499)/100 for j in _drinks]
    delivery_fee = random.randint(500,1699)/100

    order_json = {
        "order_id":     order_id,
        "foods":        foods,
        "drinks":       drinks,
        "food_prices":  food_prices,
        "drink_prices": drink_prices,
        "delivery_fee": delivery_fee,
    }
    
    _day = random.randint(1,30)
    if _day < 10: day = '0' + str(_day)
    else: day = str(_day)
    
    _month = random.randint(10,12)
    if _month < 10: month = '0' + str(_month)
    else: month = str(_month)
    
    year = '2022'

    restaurant_id = random.randint(6431256, 6832164)
    date_partition = f'{year}-{month}-{day}'
    
    data.append((restaurant_id, str(order_json), date_partition))

# Defining json schema
json_schema = StructType([
    StructField('order_id', LongType(), False),
    StructField('foods', ArrayType(StringType()), False),
    StructField('drinks', ArrayType(StringType()), False),
    StructField('food_prices', ArrayType(FloatType()), False),
    StructField('drink_prices', ArrayType(FloatType()), False),
    StructField('delivery_fee', FloatType(), False),
])

# Creating DataFrame
df = spark.createDataFrame(data=data, schema=schema)
display(df)

# Transforming into json
df_json = df.withColumn("order_json", F.from_json("order", json_schema))
display(df_json)
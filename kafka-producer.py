from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
from configparser import ConfigParser


conf_fname = "app.conf"
conf_obj = ConfigParser()
conf_read_obj = conf_obj.read(conf_fname)


#Kafka cluster/Server

kafka_host_name = conf_obj.get('kafka', 'host')
port_name = conf_obj.get('kafka', 'port_no')
kafka_topic_name = conf_obj.get('kafka', 'input_topic_name')

KAFKA_TOPIC_NAME_CONS =kafka_topic_name
KAFKA_BOOTSTRAP_SERVERS_CONS = kafka_host_name + ':' + port_name

# {"order_id": 101, "order_date": "YYYY-DD-MM HH:MM:SS", "order_amount": 100}
def remove_strip(ls):
    temp = []
    for item in ls:
        temp.append(item.replace("\n", ""))
    return temp

def concat(ls1, ls2):
    res = []
    for x, y in zip(ls1, ls2):
        res.append(f"{x}, {y}")
    return res
if __name__ == "__main__":
    kafka_producer_obj = KafkaProducer(bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer = lambda x: dumps(x).encode('utf-8'))

    product_name_list = ["Laptop", "Desktop Computer", "Mobile Phone", "Wrist Band", "Wrist Watch", "LAN Cable",
                         "HDMI Cable", "TV", "TV Stand", "Text Books", "External Hard Drive", "Pen Drive", " Online Course"]
    
    order_card_type_list = ["Visa", "Master", " Maestro", "American Express", "Cirrus", "PayPal"]

    countries_file = open("D:\\Real-timeSales\\country.txt", "r")
    countries = remove_strip(countries_file.readlines())
    cities_file = open("D:\\Real-timeSales\\city.txt", "r")
    cities = remove_strip(cities_file.readlines())
    country_name_city_name_list = concat(countries, cities)
    ecommerce_website_name_list = ["www.datamaking.com", "www.amazon.com", "www.flipkart.com", "www.snapdeal.com", "www.ebay.com"]

    message_list = []
    message = None
    for i in range(10):
        i = i + 1
        message = {}
        print("Preparing message: " + str(i))
        event_datetime = datetime.now()

        message["order_id"] = i
        message["order_product_name"] = random.choice(product_name_list)
        message["order_card_type"] = random.choice(order_card_type_list)
        message["order_amount"] = round(random.uniform( 5.5, 555.5), 2)
        message["order_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
        country_name_city_name = random.choice(country_name_city_name_list)
        country_name = country_name_city_name.split(",")[1]
        city_name = country_name_city_name.split(",")[0]
        message["order_country_name"] = country_name
        message["order_city_name"] = city_name
        message["order_ecommerce_website_name"] = random.choice(ecommerce_website_name_list)

        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        kafka_producer_obj.flush()
        time.sleep(1)


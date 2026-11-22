# Generates fake invoice data and sends it to a Kafka topic
import random
import time
from faker import Faker # library for generating fake data
from kafka import KafkaProducer # Kafka client for publishing messages
import json

fake = Faker()

# connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # change this to your kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # convert dict to json bytes
)


# makes random invoices
def generate_random_invoice():
    invoice = {
        # invoice header
        "InvoiceNumber": fake.random_int(min=1, max=99999999),
        "CreatedTime": time.ctime(),
        "CustomerCardNo": fake.credit_card_number(),
        "TotalAmount": round(random.uniform(10.0, 1000.0), 2),
        "NumberOfItems": random.randint(1, 10),  # random, not tied to line items
        "PaymentMethod": random.choice(["Cash", "Credit Card", "Debit Card", "UPI"]),
        "TaxableAmount": round(random.uniform(0.0, 800.0), 2),
        "CGST": round(random.uniform(0.0, 100.0), 2),
        "SGST": round(random.uniform(0.0, 100.0), 2),
        "CESS": round(random.uniform(0.0, 50.0), 2),

        # store, terminal and staff identifiers
        "StoreID": fake.uuid4(),
        "PosID": fake.uuid4(),
        "CashierID": fake.uuid4(),

        # customer and delivery details
        "CustomerType": random.choice(["Regular", "VIP", "Prime"]),
        "DeliveryType": random.choice(["Home Delivery", "Pickup"]),
        "DeliveryAddress": {
            "AddressLine": fake.address(),
            "City": fake.city(),
            "State": fake.state(),
            "PinCode": fake.zipcode(),
            "ContactNumber": fake.phone_number()
        },

        # 1 to 5 randomly generated products per invoice
        "InvoiceLineItems": [
            {
                "ItemCode": fake.uuid4(),
                "ItemDescription": fake.word() + " " + fake.word(),
                "ItemPrice": round(random.uniform(1.0, 100.0), 2),
                "ItemQty": random.randint(1, 5),
                "TotalValue": round(random.uniform(1.0, 500.0), 2),
            } for _ in range(random.randint(1, 5))
        ]
    }
    return invoice


if __name__ == "__main__":
    while True:
        invoice = generate_random_invoice()
        # send the invoice to the 'pos' Kafka topic
        producer.send('pos', invoice)
        print("This record has been sent:", invoice)
        time.sleep(5)  # wait 5 seconds between invoices
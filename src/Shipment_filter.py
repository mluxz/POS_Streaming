from quixstreams import Application

# Kafka connection settings
app = Application(
    broker_address="localhost:9092",           # Update with your Kafka broker address
    consumer_group="pos-project-consumer-1",
    auto_offset_reset="earliest",              # Start from the beginning if no offset is saved
)

# Input and output topics
pos_topic = app.topic(name="pos")
shipment_topic = app.topic(name="shipment")

# Stream invoices from the 'pos' topic
sdf = app.dataframe(topic=pos_topic)

# Keep only home delivery invoices
sdf = sdf[sdf["DeliveryType"] == "Home Delivery"]

# Publish the filtered invoices to the 'shipment' topic
sdf = sdf.to_topic(shipment_topic)

# Print each record to the console for monitoring
sdf.print()

if __name__ == "__main__":
    app.run()
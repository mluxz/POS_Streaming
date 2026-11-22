from quixstreams import Application

# Kafka connection settings
app = Application(
    broker_address="localhost:9092",           # Update with your Kafka broker address
    consumer_group="pos-project-consumer-2",
    auto_offset_reset="earliest",              # Start from the beginning if no offset is saved
)

# Input and output topics
pos_topic = app.topic(name="pos")
loyalty_topic = app.topic(name="loyalty")

# Stream invoices from the 'pos' topic
sdf = app.dataframe(topic=pos_topic)

# Keep only Prime customers
sdf = sdf[sdf["CustomerType"] == "Prime"]

# Keep only the fields needed for loyalty tracking
sdf = sdf[["InvoiceNumber", "CustomerCardNo", "TotalAmount"]]

# Earn 20% of the total amount as loyalty points
sdf["EarnedLoyaltyPoints"] = sdf["TotalAmount"] * 0.2

# Publish the results to the 'loyalty' topic
sdf = sdf.to_topic(loyalty_topic)

# Print each record to the console for monitoring
sdf.print()

if __name__ == "__main__":
    app.run()
from quixstreams import Application

# Kafka connection settings
app = Application(
    broker_address="localhost:9092",           # Update with your Kafka broker address
    consumer_group="pos-project-consumer-3",
    auto_offset_reset="earliest",              # Start from the beginning if no offset is saved
)

# Input and output topics
pos_topic = app.topic(name="pos")
hadoopsink_topic = app.topic(name="hadoop-sink")


def mask_card_number(card_no: str) -> str:
    #Keep only the last 4 digits, e.g. ************1234.
    card_no = str(card_no)
    return "*" * (len(card_no) - 4) + card_no[-4:]


def expand_invoicelineitems(value: dict) -> list[dict]:
    """
    Flatten one invoice into a list of records, one per line item.

    Each record combines the main invoice fields with the fields of a
    single line item.
    """
    items = [
        {
            # Invoice-level fields repeated on every line item record
            'InvoiceNumber': value['InvoiceNumber'],
            "TotalAmount": value["TotalAmount"],
            "NumberOfItems": value["NumberOfItems"],
            "TaxableAmount": value["TaxableAmount"],
            "StoreID": value["StoreID"],
            "CustomerType": value["CustomerType"],
            "DeliveryType": value["DeliveryType"],
            "CustomerCardNo": mask_card_number(value["CustomerCardNo"]),  # Masked card number
            **item  # Line item fields (ItemCode, ItemDescription, ItemPrice, ...)
        } for item in value['InvoiceLineItems']
    ]
    return items


# Stream invoices from the 'pos' topic
sdf = app.dataframe(topic=pos_topic)

# Turn each invoice into multiple records (expand=True emits one message per list item)
sdf = sdf.apply(expand_invoicelineitems, expand=True)

# Print each record to the console for monitoring
sdf.print()

# Publish the flattened records to the 'hadoop-sink' topic
sdf.to_topic(hadoopsink_topic)

if __name__ == "__main__":
    app.run()
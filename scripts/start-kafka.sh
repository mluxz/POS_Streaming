#!/usr/bin/env bash

# ---------------------------------------------------------------
# Usage:
#   1. Download Kafka and note the folder path.
#   2. Edit the paths below to match your machine.
#   3. Run: chmod +x start-kafka.sh && ./start-kafka.sh
# ---------------------------------------------------------------

# Step 1: generate a cluster ID
KAFKA_CLUSTER_ID="$(/path/to/kafka/bin/kafka-storage.sh random-uuid)"

# Step 2: format the storage (first run only)
# If want 3 nodes you can uncomment the following lines
/path/to/kafka/bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c /path/to/kafka/config/server.properties
# /path/to/kafka/bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c /path/to/kafka/config/server.properties
# /path/to/kafka/bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c /path/to/kafka/config/server.properties




# Step 3: start the server
# If want 3 nodes you can uncomment the following lines
 /path/to/kafka/bin/kafka-server-start.sh /path/to/kafka/config/server.properties
# /path/to/kafka/bin/kafka-server-start.sh /path/to/kafka/config/server.properties
# /path/to/kafka/bin/kafka-server-start.sh /path/to/kafka/config/server.properties
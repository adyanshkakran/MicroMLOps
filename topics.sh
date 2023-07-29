#!/bin/bash
KAFKA_SERVER=localhost:9092
/bin/kafka-topics --create --topic data_preprocessing --if-not-exists --bootstrap-server ${KAFKA_SERVER}
/bin/kafka-topics --create --topic feature_extraction --if-not-exists --bootstrap-server ${KAFKA_SERVER}
/bin/kafka-topics --create --topic training --if-not-exists --bootstrap-server ${KAFKA_SERVER}
/bin/kafka-topics --create --topic model_management --if-not-exists --bootstrap-server ${KAFKA_SERVER}
/bin/kafka-topics --create --topic result --if-not-exists --bootstrap-server ${KAFKA_SERVER}
/bin/kafka-topics --create --topic inference --if-not-exists --bootstrap-server ${KAFKA_SERVER}
/bin/kafka-topics --create --topic logs --if-not-exists --bootstrap-server ${KAFKA_SERVER}
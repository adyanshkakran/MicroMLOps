#!/bin/bash
KAFKA_SERVER=localhost:9092
topics=("data_preprocessing" "feature_extraction" "model_management" "result")
modes=("training" "inference")
/bin/kafka-topics --create --topic logs --if-not-exists --bootstrap-server ${KAFKA_SERVER}
/bin/kafka-topics --create --topic training --if-not-exists --bootstrap-server ${KAFKA_SERVER}
/bin/kafka-topics --create --topic inference --if-not-exists --bootstrap-server ${KAFKA_SERVER}

for topic in ${topics[@]}; do
    for mode in ${modes[@]}; do
        echo "${mode}_${topic}"
        /bin/kafka-topics --create --topic ${mode}_${topic} --if-not-exists --bootstrap-server ${KAFKA_SERVER}
    done 
done

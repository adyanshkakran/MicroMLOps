bin/kafka-topics.sh --create --topic data_preprocessing --if-not-exists --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic feature_extraction --if-not-exists --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic training --if-not-exists --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic model_management --if-not-exists --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic result --if-not-exists --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic inference --if-not-exists --bootstrap-server localhost:9092
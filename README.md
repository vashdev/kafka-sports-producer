# kafka-sports-producer
SETUP:

install kafka..
update cfgs
Start Zookeeper
C:\kafka_2.12-2.0.0\bin\windows>zookeeper-server-start   C:\kafka_2.12-2.0.0\config\zookeeper.properties
Start Kafka:
C:\kafka_2.12-2.0.0\bin\windows> kafka-server-start.bat    C:\kafka_2.12-2.0.0\config\ server.properties
create topic
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic sports --create --partitions 3 --replication-factor 1
setup cloud elastic server(bonsai)

Run TwitterSportsProducer
Run ElasticSearchConsumer
check elastic search console 
get            /_cat/nodes?v
get /sportsconsumer/cricket/2gWO5G4B2phYsw1R1CG0
/sportsconsumer/cricket/Yfmz5G4BBjI6lK5IU7zX

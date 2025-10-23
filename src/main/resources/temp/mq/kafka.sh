## kafka：主要用于监控性能，调用链日历数据传输解耦
配置存放kafka-log日志数据目录: E:\kafka\setup\data\kafkalogs
配置存放zk-log日志数据目录: E:\kafka\setup\data\zklogs

启动zk: cd E:\kafka\setup\kafka_2.11-2.1.1 ---> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
启动kafka: cd E:\kafka\setup\kafka_2.11-2.1.1 --->  .\bin\windows\kafka-server-start.bat .\config\server.properties

测试创建topic: cd E:\kafka\setup\kafka_2.11-2.1.1 --->
.\bin\windows\kafka-topics.bat --create --topic air_log_topic --zookeeper localhost:2181 --partitions 2 --replication-factor 1
.\bin\windows\kafka-topics.bat --create --topic air_alarm_topic --zookeeper localhost:2181 --partitions 1 --replication-factor 1

启动cmd--->测试启动生产者发布消息： cd E:\kafka\setup\kafka_2.11-2.1.1 --->.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic air_log_topic
输入: test
启动cmd--->测试启动消费者消费消息： cd E:\kafka\setup\kafka_2.11-2.1.1 --->.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic air_log_topic --from-beginning
接收到:test

查看topic
.\bin\windows\kafka-topics.bat --list --zookeeper localhost:2181

查看consumer-group



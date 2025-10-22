cd E:\rocketmq\rocketmq-setup\rocketmq-all-5.3.3-bin-release\bin
# windows命令窗口

# 检查broker进程是否正常运行
mqadmin clusterList -n 127.0.0.1:9876



#way1:使用集群模式创建
#-c DefaultCluster：指定集群名称，默认为DefaultCluster。
#-n 127.0.0.1:9876：指定NameServer地址，与您的配置一致。
#-t <Topic名称>：指定要创建的Topic名称。
#-r 8 -w 8：分别指定读队列和写队列的数量，这里都设置为8。

mqadmin updateTopic -c DefaultCluster -n 127.0.0.1:9876 -t TC_order_create_topic -r 2 -w 2
mqadmin updateTopic -c DefaultCluster -n 127.0.0.1:9876 -t TC_order_expired_delay_topic -r 1 -w 1
mqadmin updateTopic -c DefaultCluster -n 127.0.0.1:9876 -t TC_payment_topic -r 1 -w 1
mqadmin updateTopic -c DefaultCluster -n 127.0.0.1:9876 -t session_expired_delay_topic -r 1 -w 1
mqadmin updateTopic -c DefaultCluster -n 127.0.0.1:9876 -t sms_notification_topic -r 1 -w 1
mqadmin updateTopic -c DefaultCluster -n 127.0.0.1:9876 -t web_notification_web_topic -r 1 -w 1


#way2:使用指定Broker地址模式
updateTopic -b 127.0.0.1:10911 -n 127.0.0.1:9876 -t TC_order_create_topic -r 8 -w 8

#删除topic
deleteTopic -c DefaultCluster -n 127.0.0.1:9876 -t <Topic名称>
#查看 Topic 列表
topicList -n 127.0.0.1:9876

#查看消费者进度
mqadmin consumerProgress -g consumer-TC-payment-expired-group -n 127.0.0.1:9876

#查看消费者组连接状态
mqadmin consumerConnection -g consumer-TC-payment-expired-group -n 127.0.0.1:9876



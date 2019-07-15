# kafka-xclient

kafka-xclient is a third-part enhanced edition of Apache official kafka-clients.

The 'Best practice' of kafka consumer and producer.   

1. 提供 Consumer、Producer 易用的 API 模板;
1. 支持集中控制 consumer group 的 topic;
1. 支持多个 consumer groups，包括配置、启动、停止、查看线程状态；
1. 支持安全关闭 consumer； 
1. 支持立即消费自动创建的 topic 的数据。 
1. 支持 offset 的自动提交；
1. 支持 offset 的记录和检查，项目启动时 topic 可以自动 seek 到 上一次的 offset。
1. 全面的关键日志、统计输出，让你对消费情况了如指掌。 
1. 支持消费超时之前自动 pause 避免 re-blance;
1. 基于长期生产实践保证数据不重复、不丢失。

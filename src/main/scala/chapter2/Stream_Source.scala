package chapter2
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
object Stream_Source {
    def main(args: Array[String]): Unit = {
        //创建流处理的程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //基于本地集合
        /*val data: DataStream[String] = env.fromCollection(List("hadoop","hive"))
        //打印输出
        data.print()
        //调用execute方法
        env.execute()*/

        //基于本地文件
        /*val file: DataStream[String] = env.readTextFile("E:\\data\\hello.txt")
        //打印输出
        file.print()
        //调用execute方法
        env.execute()*/

        //基于hdfs
        /*val file: DataStream[String] = env.readTextFile("hdfs://node01:8020/wordcount/input/words.txt")
        //打印输出
        file.print()
        //调用execute方法
        env.execute()*/

        //基于kafka
        //连接kafka所需配置文件
        val props = new Properties()
        //连接kafka集群
        props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
        //消费者组id
        props.setProperty("group.id", "consumer-group")
        //key的反序列化
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        //value的反序列化
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        //从最新的数据开始消费
        props.setProperty("auto.offset.reset", "latest")
        //因为kafka是高级数据源，所以要添加数据源
        val file: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("hadoop",new SimpleStringSchema(),props))
        //打印输出
        file.print()
        //调用execute方法
        env.execute()
    }

}

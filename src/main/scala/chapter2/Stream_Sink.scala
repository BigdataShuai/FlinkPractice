package chapter2
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
object Stream_Sink {
    def main(args: Array[String]): Unit = {
        //创建流处理程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //将数据保存到kafka
        val props = new Properties()
        props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
        props.setProperty("group.id", "consumer-group")
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("auto.offset.reset", "latest")
        val listDataStream: DataStream[String] = env.fromCollection(List("hadoop","spark"))
        //因为kafka是高级数据源，添加sink
        listDataStream.addSink(new FlinkKafkaProducer010[String]("hadoop",new SimpleStringSchema(),props))
        //调用execute方法
        env.execute()
    }

}

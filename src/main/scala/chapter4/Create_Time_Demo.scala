package chapter4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Create_Time_Demo {
    def main(args: Array[String]): Unit = {
        //创建流式计算程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    }

}

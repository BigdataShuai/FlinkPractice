package chapter4
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable
object PeriodicWaterMarkOps {
    def main(args: Array[String]): Unit = {
        //创建程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //指定并行度
        env.setParallelism(1)
        //接收数据
        val data: DataStream[String] = env.socketTextStream("node01",9999)
        //切分
        val spliData: DataStream[(String, Long, Int)] = data.map(text => {
            val arr: Array[String] = text.split(" ")
            (arr(0), arr(1).toLong, 1)
        })
        //指定水印延迟时长
        val waterMark: DataStream[(String, Long, Int)] = spliData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(2)) {
            override def extractTimestamp(element: (String, Long, Int)): Long = {
                return element._2
            }
        })
        //分流
        val keyed: KeyedStream[(String, Long, Int), Tuple] = waterMark.keyBy(0)
        //打印输出
        keyed.print("keyed::")
        //指定滚动窗口
        val window: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = keyed.window(TumblingEventTimeWindows.of(Time.seconds(2)))
            //允许迟到数据的时长
            .allowedLateness(Time.seconds(3))
        //将同一个窗口当中数据的时间戳收集到set集合
        val result: DataStream[mutable.HashSet[Long]] = window.fold(new mutable.HashSet[Long]()) {
            case (set, (word, ts, count)) => {
                set += ts
            }
        }
        //打印输出
        result.print("window::")
        //调用execute方法
        env.execute()


    }

}

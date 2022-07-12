package chapter4
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
object Session_WaterMark {
    def main(args: Array[String]): Unit = {
        //创建程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //指定并行度
        env.setParallelism(1)
        //接收数据
        val data: DataStream[String] = env.socketTextStream("node01",9999)
        //切分数据
        val spliData: DataStream[(String, Long, Int)] = data.map(text => {
            val arr: Array[String] = text.split(" ")
            (arr(0), arr(1).toLong, 1)
        })
        //指定水印延迟时长
        val waterMark: DataStream[(String, Long, Int)] = spliData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(2)) {
            //获取数据当中的时间戳用来出发窗口关窗运行
            override def extractTimestamp(element: (String, Long, Int)): Long = {
                return element._2

            }
        })
        //分流
        val keyed: KeyedStream[(String, Long, Int), Tuple] = waterMark.keyBy(0)
        //打印输出
        keyed.print("keyed::")
        //指定会话窗口的会话间隔时长
        val window: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = keyed.window(EventTimeSessionWindows.withGap(Time.seconds(2)))
        //将同一个窗口当中的数据的条数聚合起来
        val windowAndCount: DataStream[(String, Long, Int)] = window.reduce((text1, text2) => {
            (text1._1, 0L, text1._3 + text2._3)
        })
        //获取窗口当中的数据的次数
        val result: DataStream[Int] = windowAndCount.map(_._3)
        //打印输出
        result.print("window::")
        //调用execute方法
        env.execute()
    }

}

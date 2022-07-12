package chapter3
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
object TimeWindow_Tumb {
    def main(args: Array[String]): Unit = {
        //创建程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //加载数据
        val file: DataStream[String] = env.socketTextStream("node01",9999)
        //切分
        val spliFile: DataStream[String] = file.flatMap(_.split(" "))
        //每个单词记为1次
        val wordAndOne: DataStream[(String, Int)] = spliFile.map((_,1))
        //分流
        val keyed: KeyedStream[(String, Int), Tuple] = wordAndOne.keyBy(0)
        //指定时间窗口当中的滚动窗口
        val window: WindowedStream[(String, Int), Tuple, TimeWindow] = keyed.timeWindow(Time.seconds(5))
        //聚合
        val wordAndCount: DataStream[(String, Int)] = window.sum(1)
        //打印输出
        wordAndCount.print()
        //调用execute方法
        env.execute()
    }

}

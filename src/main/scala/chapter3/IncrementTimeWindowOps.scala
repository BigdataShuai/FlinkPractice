package chapter3
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
object IncrementTimeWindowOps {
    def main(args: Array[String]): Unit = {
        //创建程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //接收数据
        val file: DataStream[String] = env.socketTextStream("node01",9999)
        //切分
        val spliFile: DataStream[String] = file.flatMap(_.split(" "))
        //每个单词记为1次
        val wordAndOne: DataStream[(String, Int)] = spliFile.map((_,1))
        //分流
        val keyed: KeyedStream[(String, Int), Tuple] = wordAndOne.keyBy(0)
        //指定窗口
        val window: WindowedStream[(String, Int), Tuple, TimeWindow] = keyed.timeWindow(Time.seconds(5))
        //增量聚合
        val wordAndCount: DataStream[(String, Int)] = window.reduce(new ReduceFunction[(String, Int)] {
            override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
                (value1._1, value1._2 + value2._2)
            }
        })
        //打印输出
        wordAndCount.print()
        //调用execute方法
        env.execute()
    }

}

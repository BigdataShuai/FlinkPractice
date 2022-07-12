package chapter3
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
object TimeAllWindowOps {
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
        val keyed: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)
        //指定窗口
        val window: WindowedStream[(String, Int), String, TimeWindow] = keyed.timeWindow(Time.seconds(5))
        //全窗口聚合
        val wordAndCount: DataStream[(String, Int)] = window.process(new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
            override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
                //收集要输出的结果
                out.collect(key, elements.size)
            }
        })
        //打印输出
        wordAndCount.print()
        //调用execute方法
        env.execute()
    }

}

package chapter1

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
object StreamWordCount {
    def main(args: Array[String]): Unit = {
        //创建流处理程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(6)
        //加载数据
        val file: DataStream[String] = env.socketTextStream("node01",9999)
        //切分
        val spliFile: DataStream[String] = file.flatMap(_.split(" "))
        //每个单词记为1次
        val wordAndOne: DataStream[(String, Int)] = spliFile.map((_,1))
        //按照相同的key进行分流
        val keyed: KeyedStream[(String, Int), Tuple] = wordAndOne.keyBy(0)
        //按照相同的key进行聚合
        val wordAndCount: DataStream[(String, Int)] = keyed.sum(1)
        //打印输出
        wordAndCount.print().setParallelism(1)
        //调用execute方法触发执行
        env.execute()
    }

}

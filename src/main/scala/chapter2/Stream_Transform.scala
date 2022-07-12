package chapter2
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
object Stream_Transform {
    def main(args: Array[String]): Unit = {
        //创建流处理程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //union
        /*val listDataStream1: DataStream[String] = env.fromCollection(List("1","2","3","4"))
        val listDataStream2: DataStream[String] = env.fromCollection(List("mysql hive", "hbase", "hadoop", "hbase"))
        val listDataStream3: DataStream[String] = env.fromCollection(List("tom jerry", "hauhau dahuang", "xiaoming", "xiaohong"))
        //整合两条流
        /*val result: DataStream[String] = listDataStream1.union(listDataStream2)
        //打印输出
        result.print()
        //调用execute方法
        env.execute()*/
        //整合多条流
        val result: DataStream[String] = listDataStream1.union(listDataStream2,listDataStream3)
        //打印输出
        result.print()
        //调用execute方法
        env.execute()*/

        //keyBy
        /*val listDataStream: DataStream[(String, Int)] = env.fromCollection(List(("spark", 4), ("spark", 2), ("spark", 3), ("hbase", 4)))
        //将相同的key分到一个区然后进行聚合
        val keyed: KeyedStream[(String, Int), Tuple] = listDataStream.keyBy(0)
        //聚合
        val wordAndCount: DataStream[(String, Int)] = keyed.sum(1)
        //打印输出
        wordAndCount.print()
        //调用execute方法
        env.execute()*/

        //split&select
        /*val listDataStream: DataStream[(String, Int)] = env.fromCollection(List(("hdfs", 4), ("hive", 2), ("zookeeper", 3), ("hbase", 4)))
        //将流切分成两到多条流
        val result: SplitStream[(String, Int)] = listDataStream.split(iter => {
            if (iter._1.length > 5) {
                Seq("max")
            } else {
                Seq("min")
            }
        })
        //查询想要的一到多条流
        val finResult: DataStream[(String, Int)] = result.select("max","min")
        //打印输出
        finResult.print()
        //调用execute方法
        env.execute()*/

        //connect
        /*val listDataStream1: DataStream[Int] = env.fromCollection(List(1,2,3,4))
        val listDataStream2: DataStream[String] = env.fromCollection(List("mysql hive", "hbase", "hadoop", "hbase"))
        //整合两条流
        val result: ConnectedStreams[Int, String] = listDataStream1.connect(listDataStream2)
        //调用算子，将两条数据类型不同的流转换为一种类型
        val finResult: DataStream[String] = result.flatMap(new CoFlatMapFunction[Int, String, String] {
            //第一条流转换
            override def flatMap1(value: Int, out: Collector[String]): Unit = {
                out.collect(value.toString)
            }

            //第二条流转换
            override def flatMap2(value: String, out: Collector[String]): Unit = {
                out.collect(value)
            }
        })
        //打印输出
        finResult.print()
        //调用execute方法
        env.execute()*/























    }

}

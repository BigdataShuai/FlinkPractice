package chapter4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
object Demo3 {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val dataStream = env.socketTextStream("node01",9999)

        val texStream = dataStream
            .map{
                line => val words = line.split("\t")
                    (words(0).trim,words(1).trim.toLong,words(2).trim.toDouble)
            }
            .process(new MySideOutputFun)
        texStream.print("texStream::::").setParallelism(1)
        texStream.getSideOutput(new OutputTag[String]("one")).print()
        texStream.getSideOutput(new OutputTag[Double]("tow")).print()
        env.execute("Dmeo3")
    }
}
class MySideOutputFun extends ProcessFunction[(String, Long, Double),(String, Long, Double)]{
    // 定义一个侧输出标签
    lazy val one = new OutputTag[String]("one")
    lazy val tow = new OutputTag[Double]("tow")

    override def processElement(i: (String, Long, Double),
                                context: ProcessFunction[(String, Long, Double), (String, Long, Double)]#Context,
                                collector: Collector[(String, Long, Double)]): Unit = {
        if(i._3<0){
            context.output(one," le one =  "+i._3)
        }else if(i._3>0 && i._3<32) {
            context.output(tow,i._3)
        }else {
            collector.collect(i)
        }
    }
}
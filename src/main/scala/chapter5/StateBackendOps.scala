package chapter5

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
object StateBackendOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        //    env.enableCheckpointing(1000L)//间隔多久做一次Checkpointing
        //    env.setStateBackend(new MemoryStateBackend())
        //    env.setStateBackend(new FsStateBackend(""))
        //    env.setStateBackend(new RocksDBStateBackend(""))
        //指定多久做一次状态后端
        env.enableCheckpointing(1000L)
        //选择指定的状态后端
        env.setStateBackend(new FsStateBackend("file:///E://result"))
        val dataStream = env.socketTextStream("node01",9999)
        val textKeyStream = dataStream
            .map{
                line => val words = line.split("\t")
                    (words(0).trim,words(1).trim.toLong,words(2).trim.toDouble)
            }
            .keyBy(_._1)
        val result2 = textKeyStream.process(new TempChangeAlert )
        textKeyStream.print("textKeyStream ::: ").setParallelism(1)
        result2.print("result2:::").setParallelism(1)
        env.execute("Demo5")
    }
}
class TempChangeAlert extends KeyedProcessFunction[String,(String, Long, Double),String]{
    lazy val lastTemp:ValueState[Double] = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("lastTemp",Types.of[Double])
    )
    override def processElement(i: (String, Long, Double),
                                context: KeyedProcessFunction[String, (String, Long, Double), String]#Context,
                                collector: Collector[String]): Unit = {
        val preTemp = lastTemp.value()
        lastTemp.update(i._3)
        //abs：求绝对值的方法
        if((i._3-preTemp.abs)>10){
            collector.collect("当前温度 "+i._3+" 上一次温度 "+preTemp)
        }
    }
}
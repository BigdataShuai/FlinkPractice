package chapter4

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
class PorceTempFunc extends KeyedProcessFunction[String,(String,Long,Double),String]{
    //保存历史温度
    lazy val lastTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",Types.of[Double]))
    //保存定时器报警时间
    lazy val currentTime = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTime",Types.of[Long]))
    //流当中每一条元素都要调用这个方法
    override def processElement(value: (String, Long, Double), ctx: KeyedProcessFunction[String, (String, Long, Double), String]#Context, out: Collector[String]): Unit = {
        //获取历史温度值
        val perTemp: Double = lastTemp.value()
        //将新传入的温度更新成为历史温度
        lastTemp.update(value._3)
        //获取报警时间
        val currentTimeStamp: Long = currentTime.value()
        //判断
        //如果新传入的温度小于历史温度或者历史温度没有
        if (value._3<=perTemp||perTemp==0){
            //如果有定时器，删除定时器；如果没有，就拉到
            ctx.timerService().deleteProcessingTimeTimer(currentTimeStamp)
            //清空定时器状态
            currentTime.clear()
        }else if (value._3>perTemp&&currentTimeStamp==0){
            //指定报警时间
            val timeTs: Long = ctx.timerService().currentProcessingTime()+1000L
            //注册报警器
            ctx.timerService().registerProcessingTimeTimer(timeTs)
            //报警时间更新到报警器
            currentTime.update(timeTs)
        }
    }

    //如果报警，就调用回调函数；如果没有报警，就不调用回调函数
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long, Double), String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect("当前温度已经上升了，赶紧来看一看！！！")
        //清空报警器状态
        currentTime.clear()
    }
}
object enter11{
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
        val spliData: DataStream[(String, Long, Double)] = data.map(text => {
            val arr: Array[String] = text.split("\t")
            (arr(0), arr(1).toLong, arr(2).toDouble)
        })
        //分流
        val keyed: KeyedStream[(String, Long, Double), String] = spliData.keyBy(_._1)
        //打印输出
        keyed.print("keyed::")
        //让输入的数据流调用processelement方法
        val result: DataStream[String] = keyed.process(new PorceTempFunc)
        //打印输出
        result.print("window::")
        //调用execute方法
        env.execute()

    }
}
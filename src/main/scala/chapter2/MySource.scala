package chapter2

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
//自定义source源
class MySource extends SourceFunction[String]{
    //定义运行标识
    var running = true
    //取消运行
    override def cancel(): Unit = {
        running=false
    }
    //产生数据
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val data: Range.Inclusive = 1 to 10
        while (running){
            data.foreach(t=>{
                //收集要输出的数据
                ctx.collect(t.toString)
            })
        }
    }
}
object enter{
    def main(args: Array[String]): Unit = {
        //创建流处理程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //设置并行度
        env.setParallelism(1)
        //加载数据源
        val file: DataStream[String] = env.addSource(new MySource)
        //打印输出
        file.print()
        //调用execute方法
        env.execute()
    }
}
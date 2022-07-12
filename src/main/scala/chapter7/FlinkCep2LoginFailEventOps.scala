package chapter7
import java.util

import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: String)
//最后生成的警告信息
case class Warning(userId: String, firstEventTime: String, secondEventTime: String, msg: String)
object FlinkCep2LoginFailEventOps {
    def main(args: Array[String]): Unit = {
        //创建流处理程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //设置并行度
        env.setParallelism(1)
        //加载数据
        val loginStream  = env.fromCollection(List(
            LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
            LoginEvent("1", "192.168.0.2", "success", "1558430843"),
            LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
            LoginEvent("2", "192.168.10.10", "success", "1558430845"),
            LoginEvent("1", "192.168.0.4", "fail", "1558430846")
        )
        ).assignAscendingTimestamps(_.eventTime.toLong)
        //规定匹配规则
        val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
            .where(log => log.eventType == "fail")
            .followedByAny("next")
            .where(log => log.eventType == "fail")
            .within(Time.seconds(10))
        //通过CEP进行匹配
        val result: PatternStream[LoginEvent] = CEP.pattern(loginStream,pattern)
        //查询告警信息
        val finResult: DataStream[Warning] = result.select(new MySelectPatternFunction)
        //打印输出
        finResult.print()
        //调用execute方法
        env.execute()

    }
}
class MySelectPatternFunction extends PatternSelectFunction[LoginEvent,Warning]{
    override def select(pattern: util.Map[String, util.List[LoginEvent]]): Warning = {
        //获取第一条数据
        val firstEvent: LoginEvent = pattern.getOrDefault("begin",null).get(0)
        //获取第二条数据
        val secondEvent: LoginEvent = pattern.getOrDefault("next",null).get(0)
        //获取封装的信息
        val userId: String = firstEvent.userId
        val firstEventTime: String = firstEvent.eventTime
        val secondEventTime: String = secondEvent.eventTime
        //将信息封装到样例类当中
        Warning(userId,firstEventTime,secondEventTime,"已经连续两次失败了，赶快来看一看！！！")
    }
}
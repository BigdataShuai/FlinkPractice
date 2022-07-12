package chapter6
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

case class UserLogin(platform: String, server: String, uid: String,  dataUnix: Int, status: String)
object FlinkTrumblingWindowTableOps {
    def main(args: Array[String]): Unit = {
        //创建流处理程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //创建流处理Table执行环境
        val sEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
        //指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //设置并行度
        env.setParallelism(1)
        //加载数据
        val data: DataStream[String] = env.socketTextStream("node01",9999)
        //针对数据进行操作
        val spliData: DataStream[UserLogin] = data.map(line => {
            val arr: Array[String] = line.split("\t")
            UserLogin(arr(0), arr(1), arr(2), arr(3).toInt, arr(4))
        })
        //加上水印延迟
        val water: DataStream[UserLogin] = spliData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserLogin](Time.seconds(2)) {
            override def extractTimestamp(element: UserLogin): Long = {
                return element.dataUnix
            }
        })
        //将数据转换为table
        //val table: Table = sEnv.fromDataStream(water,'platform,'server,'status,'ts.rowtime)

        //使用sql风格
        sEnv.registerDataStream("t_time",water,'platform,'server,'status,'ts.rowtime)
        //抒写sql语句
        val sql =
            s"""
              |select platform,count(1) as counts
              |from t_time
              |where status = 'LOGIN'
              |group by platform,tumble(ts,interval '2' second)
            """.stripMargin
        //使用sql语句
        val result: Table = sEnv.sqlQuery(sql)
        //将结果放入流中
        val finResult: DataStream[(Boolean, Row)] = sEnv.toRetractStream[Row](result)
        //打印输出
        finResult.print()
        //调用execute方法
        env.execute()
    }

}

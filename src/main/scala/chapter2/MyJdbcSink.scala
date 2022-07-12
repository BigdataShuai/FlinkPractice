package chapter2

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
//继承富函数
//富函数：有生命周期，有开始，有中间，有结束
class MyJdbcSink extends RichSinkFunction[String]{
    //打开数据库连接
    var conn:Connection=_
    var ps:PreparedStatement=_
    override def open(parameters: Configuration): Unit = {
        conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root","root")
        ps = conn.prepareStatement("insert into infotest values (?)")

    }
    //插入数据
    override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
        ps.setString(1,value)
        ps.execute()
    }
    //断开数据库连接
    override def close(): Unit = {
        ps.close()
        conn.close()
    }
}
object enters{
    def main(args: Array[String]): Unit = {
        //创建流处理程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //加载数据
        val listDataSream: DataStream[String] = env.fromCollection(List("hadoop","spark"))
        //保存数据
        listDataSream.addSink(new MyJdbcSink)
        //调用execute方法
        env.execute()

    }
}
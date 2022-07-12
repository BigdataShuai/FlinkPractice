import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
case class Person(id: Int, name: String, age: Int)
class ClickHouseSink extends RichSinkFunction[Person]{
    var conn:Connection=_
    var ps:PreparedStatement=_
    //连接数据库
    override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:clickhouse://node01:8123/test","default",null)
        ps = conn.prepareStatement("insert into person values(?,?,?)")
    }
    //插入数据
    override def invoke(value: Person, context: SinkFunction.Context[_]): Unit = {
        ps.setInt(1,value.id)
        ps.setString(2,value.name)
        ps.setInt(3,value.age)
        //执行插入操作
        ps.execute()
    }
    //断开数据库连接
    override def close(): Unit = {
        ps.close()
        conn.close()
    }
}
object enter{
    def main(args: Array[String]): Unit = {
        //创建程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //加载数据
        val inputs = env.fromCollection(List(
            Person(9, "jack", 17),
            Person(10, "tom", 18),
            Person(11, "lucy", 19)
        ))
        //将数据保存到数据库
        inputs.addSink(new ClickHouseSink)
        //调用execute方法
        env.execute()
    }
}
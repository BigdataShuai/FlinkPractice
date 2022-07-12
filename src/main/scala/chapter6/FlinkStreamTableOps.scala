package chapter6
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
case class Goods(id: Int,brand:String,category:String)
object FlinkStreamTableOps {
    def main(args: Array[String]): Unit = {
        //创建流处理程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //创建流处理Table程序入口
        val sEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
        //加载数据
        val dataStream: DataStream[String] = env.fromElements(
            "001|mi|mobile",
            "002|mi|mobile",
            "003|mi|mobile",
            "004|mi|mobile",
            "005|huawei|mobile",
            "006|huawei|mobile",
            "007|huawei|mobile",
            "008|Oppo|mobile",
            "009|Oppo|mobile",
            "010|uniqlo|clothing",
            "011|uniqlo|clothing",
            "012|uniqlo|clothing",
            "013|uniqlo|clothing",
            "014|uniqlo|clothing",
            "015|selected|clothing",
            "016|selected|clothing",
            "017|selected|clothing",
            "018|Armani|clothing",
            "019|lining|sports",
            "020|nike|sports",
            "021|adidas|sports",
            "022|nike|sports",
            "023|anta|sports",
            "024|lining|sports"
        )
        //针对数据进行切分
        val spliData: DataStream[Goods] = dataStream.map(text => {
            val arr: Array[String] = text.split("\\|")
            Goods(arr(0).toInt, arr(1), arr(2))
        })
        //将数据转换为table
        val sport: Table = sEnv.fromDataStream(spliData)
        //查询
        val result: Table = sport.select('category).distinct()
        //将查询结果加入到流中
        //toAppendStream:追加模式，不能应用于聚合操作
        //toRetractStream:缩进模式，应用于聚合操作
        val finResult: DataStream[(Boolean, Row)] = sEnv.toRetractStream[Row](result)
        //打印输出
        finResult.print()
        //调用execute方法
        env.execute()

    }

}

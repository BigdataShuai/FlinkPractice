package chapter6
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
object FlinkSQLOps {
    def main(args: Array[String]): Unit = {
        //创建批处理程序入口
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //创建批处理Table程序入口
        val bEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)
        //加载数据
        val dataStream: DataSet[String] = env.fromElements(
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
        //操作数据
        val spliData: DataSet[(Int, String, String)] = dataStream.map(line => {
            val arr: Array[String] = line.split("\\|")
            (arr(0).toInt, arr(1), arr(2))
        })
        //要想使用sql风格，需要将数据注册成为一张表
        bEnv.registerDataSet("t_sport",spliData,'id,'sport,'category)
        //抒写sql
        val sql =
            """
              |select category,count(1) as counts
              |from t_sport
              |group by category
              |order by counts desc
            """.stripMargin
        //执行sql
        val result: Table = bEnv.sqlQuery(sql)
        //将结果加入到dataSet
        val finResult: DataSet[Row] = bEnv.toDataSet[Row](result)
        //打印输出
        finResult.print()

    }

}

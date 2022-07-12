package chapter6
import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

case class UserBrowseLog(userID: Int,eventTime: String,eventType: String,productID: String, productPrice: Double)
object UDF_Demo {
    def main(args: Array[String]): Unit = {
        //创建批处理程序入口
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //创建批处理Table程序入口
        val bEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)
        //加载数据
        val ds = env.fromElements(
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:00\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 1, \"eventTime\": \"2020-10-01 10:02:02\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:06\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 1, \"eventTime\": \"2020-10-01 10:02:10\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:06\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:06\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 1, \"eventTime\": \"2020-10-01 10:02:12\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:06\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:06\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 1, \"eventTime\": \"2020-10-01 10:02:15\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 1, \"eventTime\": \"2020-10-01 10:02:16\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}"
        )
        //解析json文件
        val parseData: DataSet[UserBrowseLog] = ds.map(line => {
            val jsonObj: JSONObject = JSON.parseObject(line)
            val userID = jsonObj.getInteger("userID")
            val eventTime = jsonObj.getString("eventTime")
            val eventType = jsonObj.getString("eventType")
            val productID = jsonObj.getString("productID")
            val productPrice = jsonObj.getDouble("productPrice")
            UserBrowseLog(userID, eventTime, eventType, productID, productPrice)
        })
        //将数据注册成为一张表
        bEnv.registerDataSet("t_time",parseData)
        //注册udf函数
        bEnv.registerFunction("toTime",new ToTime)

        //抒写sql
        val sql =
            """
              |select eventTime,toTime(eventTime)
              |from t_time
            """.stripMargin
        //执行sql
        val result: Table = bEnv.sqlQuery(sql)
        //将查询结果加入dataSet
        val finResult: DataSet[Row] = bEnv.toDataSet[Row](result)
        //打印输出
        finResult.print()
    }
}
class ToTime extends ScalarFunction{
    //格式化时间
    private val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //重写eval方法
    def eval(str:String)={
        format.parse(str).getTime
    }
}

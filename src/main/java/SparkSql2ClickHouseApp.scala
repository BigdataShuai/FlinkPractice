import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSql2ClickHouseApp {
    def main(args: Array[String]): Unit = {
        //创建程序入口
        val spark: SparkSession = SparkSession.builder().appName("demo").master("local[*]").getOrCreate()
        //调用sparkContext
        val sc: SparkContext = spark.sparkContext
        //设置日志级别
        sc.setLogLevel("WARN")
        //导包
        import spark.implicits._
        //加载数据
        val persons = spark.createDataset(List(
            Person(6, "lilei", 23),
            Person(7, "hanmeimei", 24),
            Person(8, "xiaohong", 25)))
        //使用完全体将数据保存到数据库
        persons.write
            .mode(SaveMode.Append)
            .format("jdbc")
            .option("url","jdbc:clickhouse://node01:8123/test")
            .option("user","default")
            .option("dbtable","person")
            .save()
    }

}

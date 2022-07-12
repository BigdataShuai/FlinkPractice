import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}
case class Person(id: Int, name: String, age: Int)
object SparkCore2ClickHouseApp {
    def main(args: Array[String]): Unit = {
        //创建sparkCore程序入口
        val conf: SparkConf = new SparkConf().setAppName("demo").setMaster("local[*]")
        val sc = new SparkContext(conf)
        //设置日志级别
        sc.setLogLevel("WARN")
        //加载数据
        val persons = sc.parallelize(List(
            Person(1, "zhangsan", 13),
            Person(2, "lisi", 14),
            Person(3, "wangwu", 15),
            Person(4, "zhaoliu", 16),
            Person(5, "zhouqi", 17))
        )
        //遍历rdd的分区
        persons.foreachPartition(partition=>{
            //连接数据库
            val conn: Connection = DriverManager.getConnection("jdbc:clickhouse://node01:8123/test","default",null)
            //抒写sql
            val ps: PreparedStatement = conn.prepareStatement("insert into person values(?,?,?)")
            //将分区当中的数据进行保存
            partition.foreach(person=>{
                ps.setInt(1,person.id)
                ps.setString(2,person.name)
                ps.setInt(3,person.age)
                ps.execute()
            })
            //断开连接
            ps.close()
            conn.close()
        })
    }

}

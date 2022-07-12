package chapter2
import org.apache.flink.api.scala._
object Batch_Sink {
    def main(args: Array[String]): Unit = {
        //创建批处理程序入口
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        /*//加载数据
        val listDataSet: DataSet[String] = env.fromCollection(List("hadoop","spark","hive"))
        //收集数据
        val result: Seq[String] = listDataSet.collect()
        //打印输出
        result.foreach(println)*/

        //保存到本地文件
        /*val fileDataSet: DataSet[String] = env.fromCollection(List("hadoop","spark"))
        //将数据保存到本地
        fileDataSet.writeAsText("E:\\result")
        //调用execute方法
        env.execute()*/

        //将数据保存到hdfs
        val fileDataSet: DataSet[String] = env.fromCollection(List("hadoop","spark"))
        //保存数据
        fileDataSet.writeAsText("hdfs://node01:8020/wordcount/input/result")
        //调用execute方法
        env.execute()
    }

}

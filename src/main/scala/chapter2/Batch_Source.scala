package chapter2
import org.apache.flink.api.scala._
object Batch_Source {
    def main(args: Array[String]): Unit = {
        //创建批处理的程序入口
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        //本地集合
        /*val data: DataSet[String] = env.fromCollection(List("hadoop","spark"))
        //打印输出
        data.print()*/

        //基于本地文件
        /*val file: DataSet[String] = env.readTextFile("E:\\data\\hello.txt")
        //打印输出
        file.print()*/

        //基于hdfs
        /*val file: DataSet[String] = env.readTextFile("hdfs://node01:8020/wordcount/input/words.txt")
        //打印输出
        file.print()*/
    }

}

package chapter6
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
case class Student(id:Int,name:String,age:Int,gender:String,course:String,score:Int)
object FlinkBatchTableOps {
    def main(args: Array[String]): Unit = {
        //创建批处理程序入口
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //创建批处理Table的程序入口
        val bEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)
        //加载csv文件
        val file: DataSet[Student] = env.readCsvFile[Student]("E:\\data\\student.csv",fieldDelimiter="|",ignoreFirstLine=true)
        //将数据转换为table
        val student: Table = bEnv.fromDataSet(file)
        //查询数据
        val result: Table = student.select('name,'age).filter('age>20)
        //将查询结果加到DataSet中
        val finResult: DataSet[Row] = bEnv.toDataSet[Row](result)
        //打印输出
        finResult.print()


    }

}

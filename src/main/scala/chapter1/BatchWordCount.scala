package chapter1

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.api.scala._

object BatchWordCount {
    def main(args: Array[String]): Unit = {
        //创建批处理的程序入口
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        //加载数据
        val file: DataSet[String] = env.readTextFile(args(0))
        //切分数据
        val spliFile: DataSet[String] = file.flatMap(_.split(" "))
        //每个单词记为1次
        val wordAndOne: DataSet[(String, Int)] = spliFile.map((_,1))
        //按照相同的key进行分组
        val grouped: GroupedDataSet[(String, Int)] = wordAndOne.groupBy(0)
        //聚合元素
        val wordAndCount: AggregateDataSet[(String, Int)] = grouped.sum(1)
        //打印输出
        //wordAndCount.print()
        //将结果保存到指定地点
        wordAndCount.writeAsText(args(1))
        //批处理，如果要将结果保存到指定地点，必须调用execute方法
        env.execute()

    }

}

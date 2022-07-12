package chapter2
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
object Batch_Transform {
    def main(args: Array[String]): Unit = {
        //创建批处理程序入口
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        //map
        //加载数据
        /*val listDataSet: DataSet[Int] = env.fromCollection(List(1,2,3))
        //将每个元素扩大10倍
        val result: DataSet[Int] = listDataSet.map(_*10)
        //打印输出
        result.print()*/

        //flatMap
        /*val listDataSet: DataSet[String] = env.fromCollection(List("hadoop spark","hive mysql","hbase kafka"))
        //打印输出
        listDataSet.print()
        //按照分隔符进行切分
        val result: DataSet[String] = listDataSet.flatMap(_.split(" "))
        //打印输出
        result.print()*/

        //mapPartition
        /*val listDataSet: DataSet[String] = env.fromCollection(List("hadoop spark","hive mysql","hbase kafka"))
        //切分数据
        val result: DataSet[String] = listDataSet.mapPartition(iter => {
            iter.flatMap(_.split(" "))
        })
        //打印输出
        result.print()*/

        //filter
        /*val listDataSet: DataSet[String] = env.fromCollection(List("hadoop spark","hive","hbase kafka"))
        //过滤出来长度大于5的元素
        val result: DataSet[String] = listDataSet.filter(_.length>5)
        //打印输出
        result.print()*/

        //reduce
        /*val data: DataSet[Int] = env.fromCollection(List(1,2,3,4,5))
        //聚合
        val result: DataSet[Int] = data.reduce(_+_)
        //打印输出
        result.print()*/

        //reduceGroup
        /*val data: DataSet[Int] = env.fromCollection(List(1,2,3,4,5))
        //聚合
        val result: DataSet[Int] = data.reduceGroup(iter => {
            iter.reduce(_ + _)
        })
        //打印输出
        result.print()*/

        //aggregate
        //加载数据
        /*val listDataSet: DataSet[(String,Int)] = env.fromCollection(List(("java",1),("spark",2),("spark",3),("java",4)))
        //调用算子求和
        val result: AggregateDataSet[(String, Int)] = listDataSet.groupBy(0).aggregate(Aggregations.SUM,1)
        //打印输出
        result.print()*/

        //distinct
        /*val listDataSet: DataSet[(String,Int)] = env.fromCollection(List(("java",4),("spark",2),("spark",3),("java",4)))
        //去重
        val result: DataSet[(String, Int)] = listDataSet.distinct(0)
        //打印输出
        result.print()*/

        //join
        /*val listDataSet1: DataSet[(String, Int)] = env.fromCollection(List(("java", 4), ("spark", 2), ("mysql", 3), ("hbase", 4)))
        val listDataSet2: DataSet[(String, Int)] = env.fromCollection(List(("java", 1), ("spark", 1),("hive",1)))
        //join
        val result: JoinDataSet[(String, Int), (String, Int)] = listDataSet1.join(listDataSet2).where(0).equalTo(0)
        //打印输出
        result.print()*/

        //rebalance
        /*val data: DataSet[Long] = env.generateSequence(0,10)
        //将数据转换成key，value对类型
        /*val result: DataSet[(Long, Long)] = data.map(new RichMapFunction[Long, (Long, Long)] {
            override def map(value: Long): (Long, Long) = {
                //获取子任务编号
                (getRuntimeContext.getIndexOfThisSubtask, value)
            }
        })
        //打印输出
        result.print()*/
        //调用算子，尽量避免数据倾斜
        val result: DataSet[Long] = data.rebalance()
        //转换为key，value
        val finResult: DataSet[(Long, Long)] = result.map(new RichMapFunction[Long, (Long, Long)] {
            override def map(value: Long): (Long, Long) = {
                //获取子任务编号
                (getRuntimeContext.getIndexOfThisSubtask, value)
            }
        })
        //打印输出
        finResult.print()*/

        //partitionByHash
        /*val data: DataSet[Long] = env.generateSequence(0,10)
        //按照hash方式分区
        val result: DataSet[Long] = data.partitionByHash(word=>word)
        //转换为key，value
        val finResult: DataSet[(Long, Long)] = result.map(new RichMapFunction[Long, (Long, Long)] {
            override def map(value: Long): (Long, Long) = {
                (getRuntimeContext.getIndexOfThisSubtask, value)
            }
        })
        //打印输出
        finResult.print()*/

        //sortPartition
        val dataSet: DataSet[Int] = env.fromCollection(List[Int](1,2,3,4,5))
        //指定并行度
        dataSet.setParallelism(1)
        //分区排序
        val result: DataSet[Int] = dataSet.sortPartition(word=>word,Order.ASCENDING)
        //打印输出
        result.print()









    }

}

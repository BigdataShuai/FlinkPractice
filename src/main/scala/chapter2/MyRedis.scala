package chapter2

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.api.scala._
class MyRedis extends RedisMapper[String]{
    //指定保存数据的类型
    override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET,"redis")
    }
    //获取value
    override def getValueFromData(data: String): String = {
        data
    }
    //获取key
    override def getKeyFromData(data: String): String = {
        data
    }
}
object enter2{
    def main(args: Array[String]): Unit = {
        //创建流处理程序入口
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //加载数据
        val listDataStream: DataStream[String] = env.fromCollection(List("hadoop","spark"))
        //连接redis所需配置文件信息
        val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setDatabase(1).setHost("localhost").setPort(6379).build()
        //将数据保存到redis
        listDataStream.addSink(new RedisSink[String](config,new MyRedis))
        //调用execute方法
        env.execute()

    }
}
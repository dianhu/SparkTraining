package org.training.spark.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.serializer.StringDecoder
import com.alibaba.fastjson.JSON
import org.training.spark.util.{KafkaRedisProperties, RedisClient}
/**
  * Time : 16-11-3 下午5:27
  * Author : hcy
  * Description : spark streaming从kafka取数据，经过处理写入redis
  * 从redis客户端查看：
  * /usr/local/bin/ ./redis-cli
  * keys *
  * type app::users::click
  * hlen app::users::click
  * hvals app::users::click
  * hmget app::users::click 97edfc08311c70143401745a03a50706 6b67c8c700427dee7552f81f3228c927
  */
object UserClickCountAnalytics {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[4]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(4))
    ssc.checkpoint("checkpoint")

    // Kafka configurations
    val topics = KafkaRedisProperties.KAFKA_USER_TOPIC.split("\\,").toSet
    println(s"Topics: ${topics}.")

    val brokers = KafkaRedisProperties.KAFKA_ADDR
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )

    val clickHashKey = "app::users::click"

    // Create a direct stream
    val kafkaStream = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      println(s"Line ${line}.")
      val data = JSON.parseObject(line._2)
      Some(data)
    })

    // Compute user click times
    //val userClicks = events.map(x => (x.getString("uid"), x.getLong("click_count"))).reduceByKey(_ + _)

    val userClicks = events.map(x => (x.getString("uid"), x.getLong("click_count"))).window(Seconds(60),Seconds(20))reduceByKeyAndWindow(_+_,_-_,Seconds(60),Seconds(20),2)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        //val jedis = RedisClient.pool.getResource
        partitionOfRecords.foreach(pair => {
          try {
            val uid = pair._1
            val clickCount = pair._2
            //jedis.hincrBy(clickHashKey, uid, clickCount)
            println(s"Update uid ${uid} to ${clickCount}.")
          } catch {
            case e: Exception => println("error:" + e)
          }
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

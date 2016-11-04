package org.training.spark.util

/**
  * Time : 16-11-3 下午5:37
  * Author : hcy
  * Description : 
  */
object KafkaRedisProperties {
  val REDIS_SERVER: String = "127.0.0.1"
  val REDIS_PORT: Int = 6379

  val KAFKA_SERVER: String = "127.0.0.1"
  val KAFKA_ADDR: String = KAFKA_SERVER + ":9092"
  val KAFKA_USER_TOPIC: String = "user_events"
  val KAFKA_RECO_TOPIC: String = "reco4"

}

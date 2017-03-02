package org.training.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Time : 17-3-1 下午4:52
  * Author : hcy
  * Description : 
  */
object HcyDemo {

  case class User(userID: String, gender: String, age: String, occupation: String, zipcode: String)

  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    var dataPath = "data/ml-1m/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    val conf = new SparkConf().setMaster(masterUrl).setAppName("HcyDemo")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    /**
      * Create RDDs
      */
    val DATA_PATH = dataPath
    val usersRdd = sc.textFile(DATA_PATH + "users.dat")

    val schemaString = "userID gender age occupation zipcode"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val userRDD2 = usersRdd.map(_.split("::")).map(p => Row(p(0), p(1).trim, p(2).trim, p(3).trim, p(4).trim))
    val userDataFrame2 = sqlContext.createDataFrame(userRDD2, schema)
    userDataFrame2.take(10).foreach(println)
    print(userDataFrame2.count())

    userDataFrame2.registerTempTable("users")
    val groupedUsers = sqlContext.sql("select gender, age, count(*) as n from users group by gender, age")
    groupedUsers.show()

  }
}

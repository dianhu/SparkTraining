package org.training.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Time : 17-3-2 上午9:47
  * Author : hcy
  * Description : 
  */
object SparkSQLExample {
  // $example on:create_ds$
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface
  //case class Person(name:String,age:Long)

  def main(args:Array[String]): Unit ={
    var masterUrl = "local[1]"
    var dataPath = "data/ml-1m/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    val conf = new SparkConf().setMaster(masterUrl).setAppName("SparkSQLExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    runBasicDataFrameExample(sqlContext,dataPath)

    sc.stop()

  }
  def runBasicDataFrameExample(sqlContext:SQLContext,dataPath:String): Unit ={
    val df = sqlContext.read.json(dataPath+"people.json");
    // Displays the content of the DataFrame to stdout
    df.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)
    df.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    import sqlContext.implicits._
    df.select($"name",$"age"+1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+
    df.filter($"age">21).show()
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+
    df.groupBy("age").count().show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+
    df.registerTempTable("people")
    val sqlDF = sqlContext.sql("select * from people order by age")
    sqlDF.show()
    println("sql show")
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

  }

}

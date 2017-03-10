package org.training.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Time : 17-3-9 下午5:03
  * Author : hcy
  * Description : 
  */
object TransformationDemo {
  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    var dataPath = "data/ml-1m/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if(args.length > 1) {
      dataPath = args(1)
    }

    val conf = new SparkConf().setMaster(masterUrl).setAppName("TransformationDemo")
    val sc = new SparkContext(conf)

    val DATA_PATH = dataPath
    val usersRdd = sc.textFile(DATA_PATH + "people.dat")



    //users: RDD[(userName,age)]
    val users = usersRdd.map(_.split("::")).map { x =>
      (x(0), x(1))
    }
    //mapPartition
    users.mapPartitions(x=>x.++("aaaaaaa")).foreach(print)
    //filter 用法
    users.filter(x=>x._2.toInt>31).foreach(println)
    println("------------------")

    //aggregateByKey的用法
    val data=List((1,3),(1,2),(1,4),(2,3))
    val rdd=sc.parallelize(data, 2)

    //合并不同partition中的值，a，b得数据类型为zeroValue的数据类型
    def combOp(a:String,b:String):String={
      println("combOp: "+a+"\t"+b)
      a+b
    }
    //合并在同一个partition中的值，a的数据类型为zeroValue的数据类型，b的数据类型为原value的数据类型
    def seqOp(a:String,b:Int):String={
      println("SeqOp:"+a+"\t"+b)
      a+b
    }
    rdd.foreach(println)
    //zeroValue:中立值,定义返回value的类型，并参与运算
    //seqOp:用来在同一个partition中合并值
    //combOp:用来在不同partiton中合并值
    val aggregateByKeyRDD=rdd.aggregateByKey("100")(seqOp, combOp)
    aggregateByKeyRDD.foreach(print)
    sc.stop()
  }

}

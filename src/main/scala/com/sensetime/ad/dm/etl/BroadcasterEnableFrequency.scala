package com.sensetime.ad.dm.etl

import com.sensetime.ad.dm.utils.Redis
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.util.{Failure, Success, Try}

/**
  * Created by yuanpingzhou on 2/24/17.
  */
object BroadcasterEnableFrequency {
  private val host: String = "localhost"
  private val port: Int = 6379
  private val dbNum: Int = 3

  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("params: mode[local|yarn] InputDir OutputDir")
      System.exit(1)
    }

    val mode = args(0)
    val inputDir = args(1)
    val outputDir = args(2)

    // spark environment
    val conf = new SparkConf().setMaster(mode).setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // load data
    val sqlContext = new SQLContext(sc)
    val adPushData = sqlContext.read.json(s"${inputDir}/broadcaster_ad_show")
    println(s"columns ${adPushData.columns}")
    println(s"${adPushData.count()}")

    val broadcasterPartitioner = new HashPartitioner(4)
    adPushData.rdd.map{
      case record => {
        val appId = record.getString(0)
        val broadcasterId = record.getString(1)
        val adId = record.getString(2)
        (s"${appId}_${broadcasterId}:${adId}",1)
      }
    }.reduceByKey((a,b) => a+b).map{
      case (name,count) =>{
        val parts = name.split(":")
        (parts(0),(parts(1),count))
      }
    }.partitionBy(broadcasterPartitioner).groupByKey.foreachPartition{
      case p =>{
        val dbHandle = new Redis(this.host,this.port)
        var freq = Map[String,Map[String,Int]]()
        p.foreach{
          case (k,v) =>{
            val ret = v.toMap
            freq ++= Map(k -> ret)
          }
        }
        Try(dbHandle.insertMultHashRecord(dbNum,freq)) match {
          case Success(v) =>
            println("Insert frequency success .")
            dbHandle.disconnect()
          case Failure(msg) =>
            dbHandle.disconnect()
            println(s"Insert frequency failed : ${msg.getMessage}")
            System.exit(1)
        }
      }
    }

  }

}

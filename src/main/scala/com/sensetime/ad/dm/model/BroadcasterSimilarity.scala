package com.sensetime.ad.dm.model

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg.{DenseVector => BDV}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Try,Failure,Success}

import com.sensetime.ad.dm.utils._

/**
  * Created by yuanpingzhou on 2/22/17.
  */
object BroadcasterSimilarity {
  private val host: String = "localhost"
  private val port: Int = 6379
  private val dbNum: Int = 1

  def insertSimilarity(sim: Map[String,Map[String,Int]]): Unit = {
    val dbHandle = new Redis(this.host,this.port)
    Try(dbHandle.insertMultHashRecord(dbNum,sim)) match {
      case Success(v) =>
        println("Insert similarity success .")
      case Failure(msg) =>
        dbHandle.disconnect()
        println(s"Insert similarity failed : ${msg.getMessage}")
        System.exit(1)
    }
  }

  def main(args: Array[String]) = {

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
    val userInfoJson = sqlContext.read.json(s"${inputDir}/dm_broadcaster_info")
    println(s"columns ${userInfoJson.columns}")
    println(s"${userInfoJson.count()}")

    // feature space construction
    var userFeatVec = Map[String,BDV[Int]]()
    val users = ArrayBuffer[String]()
    userInfoJson.rdd.foreach{
      case v => {
        val featVec = BDV.zeros[Int](39)
        val userId = v.getString(0)
        users.append(userId)
        var idx = 1
        // gender
        featVec(idx + v.getInt(1)) = 1
        idx += 3
        // ageGroup
        v.getInt(2) match{
          case ageGroup: Int if(ageGroup > 0) => featVec(idx + ageGroup - 1) = 1
          case ageGroup: Int => featVec(idx + ageGroup) = 1
        }
        idx += 6
        // os
        v.getInt(3) match{
          case os: Int if(os > 0) => featVec(idx + os - 1) = 1
          case os: Int => featVec(idx + os) = 1
        }
        idx += 3
        // networkType
        v.getInt(4) match{
          case networkType: Int if(networkType > 0) => featVec(idx + networkType - 1) = 1
          case networkType: Int => featVec(idx + networkType) = 1
        }
        idx += 3
        // fansCountLevel
        v.getInt(5) match{
          case fansCountLevel: Int if(fansCountLevel > 0) => featVec(idx + fansCountLevel - 1) = 1
          case fansCountLevel: Int => featVec(idx + fansCountLevel) = 1
        }
        idx += 6
        // enableTimes
        v.getInt(6) match{
          case enableTimes: Int if(enableTimes > 0) => featVec(idx + enableTimes - 1) = 1
          case enableTimes: Int => featVec(idx + enableTimes) = 1
        }
        idx += 6
        // postTimes
        v.getInt(7) match{
          case postTimes: Int if(postTimes > 0) => featVec(idx + postTimes - 1) = 1
          case postTimes: Int => featVec(idx + postTimes) = 1
        }
        idx += 6
        // enableRate
        v.getInt(8) match{
          case enableRate: Int if(enableRate > 0) => featVec(idx + enableRate - 1) = 1
          case enableRate: Int => featVec(idx + enableRate) = 1
        }
        userFeatVec ++= Map(userId -> featVec)
      }
    }

    // similarity computation
    var similarity = Map[String,Map[String,Int]]()
    var i = 0
    while(i < users.length){
      val aUser = users(i)
      val aUserFeat = userFeatVec.get(aUser).get
      var j = i + 1
      var feats = Map[String,Int]()
      while(j < users.length){
        val bUser = users(j)
        val bUserFeat = userFeatVec.get(bUser).get
        val sim = aUserFeat.dot(bUserFeat)
        //similarity ++= Map(s"${aUser}:${bUser}" -> sim)
        feats ++= Map(bUser -> sim)
        j += 1
      }
      similarity ++= Map(aUser -> feats)
      i += 1
    }

    //
    insertSimilarity(similarity)

  }
}

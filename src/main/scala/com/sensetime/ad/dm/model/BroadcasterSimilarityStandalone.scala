package com.sensetime.ad.dm.model

/**
  * Compute broadcaster similarity through inner multiplication in standalone mode, since resources would be intensely
  * consumed in cluster while transposing matrix before computing cosine similarity.
  *
  * Created by yuanpingzhou on 2/22/17.
  */
object BroadcasterSimilarityStandalone {

  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.sql.hive.HiveContext
  import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry, RowMatrix}

  import scala.util.{Failure, Success, Try}
  import breeze.linalg.{DenseVector => BDV}
  import scopt.OptionParser

  import com.sensetime.ad.dm.utils._

  case class Parameter(redisHost: String = "10.0.8.81", redisPort: Int = 6379, redisDB: Int = 10,
                       hiveDBName: String = "dm", hiveTableName: String = "dm_broadcaster_info")

  def main(args: Array[String]) = {

    val defaultParams = Parameter()

    val parser = new OptionParser[Parameter]("BroadcasterSimilarityStandalone") {
      head("BroadcasterSimilarityStandalone: dm app .")
      opt[String]("redisHost")
        .required()
        .text(s"Hostname or IP for Redis server")
        .action((x, c) => c.copy(redisHost = x))
      opt[Int]("redisPort")
        .required()
        .text(s"Port for Redis server")
        .action((x, c) => c.copy(redisPort = x))
      opt[Int]("redisDB")
        .required()
        .text(s"DB number of Redis ")
        .action((x, c) => c.copy(redisDB = x))
      opt[String]("hiveDBName")
        .required()
        .text(s"DB name of Hive ")
        .action((x, c) => c.copy(hiveDBName = x))
      opt[String]("hiveTableName")
        .required()
        .text(s"Table name of Hive ")
        .action((x, c) => c.copy(hiveTableName = x))
      note(
        """
          |For example, the following command runs this app on hive dataset:
          |
          | ./bin/spark-submit  --class  com.sensetime.ad.dm.model.BroadcasterSimilarityStandalone \
          | recommendation-2.1.3.jar \
          | --redisHost 10.0.8.81 --redisPort 6379 --redisDB 10 --hiveDBName dm --hiveTableName dm_broadcaster_info
        """.stripMargin)
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Parameter): Unit = {
    // spark environment
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.dialect", "hiveql")
    sqlContext.sql(s"use ${params.hiveDBName}")
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    // load data
    // feature space construction
    val userFeatures = sqlContext.sql(s"select * from ${params.hiveTableName}").map(
      v => {
        val featVec = BDV.zeros[Double](39)
        val userId = v.getString(0)
        var idx = 1
        // gender 3
        featVec(idx + v.getInt(2)) = 1
        idx += 3
        // ageGroup 6
        v.getByte(1).toInt match {
          case ageGroup: Int if (ageGroup > 0) => featVec(idx + ageGroup - 1) = 1.0
          case ageGroup: Int => featVec(idx + ageGroup) = 1.0
        }
        idx += 6
        // os 3
        v.getByte(4).toInt match {
          case os: Int if (os > 0) => featVec(idx + os - 1) = 1.0
          case os: Int => featVec(idx + os) = 1.0
        }
        idx += 3
        // networkType 3
        v.getByte(5).toInt match {
          case networkType: Int if (networkType > 0) => featVec(idx + networkType - 1) = 1.0
          case networkType: Int => featVec(idx + networkType) = 1.0
        }
        idx += 3
        // fansCountLevel 6
        v.getByte(3).toInt match {
          case fansCountLevel: Int if (fansCountLevel > 0) => featVec(idx + fansCountLevel - 1) = 1.0
          case fansCountLevel: Int => featVec(idx + fansCountLevel) = 1.0
        }
        idx += 6
        // TODO,
        // enableTimes 6
        v.getLong(6).toInt match {
          case enableTimes: Int if (enableTimes <= 0) => featVec(idx + 0 - 1) = 1.0
          case enableTimes: Int if ((enableTimes > 0) && (enableTimes < 100)) => featVec(idx + 1 - 1) = 1.0
          case enableTimes: Int if ((enableTimes >= 100) && (enableTimes < 200)) => featVec(idx + 2 - 1) = 1.0
          case enableTimes: Int if ((enableTimes >= 200) && (enableTimes < 500)) => featVec(idx + 3 - 1) = 1.0
          case enableTimes: Int if ((enableTimes >= 500) && (enableTimes < 1000)) => featVec(idx + 4 - 1) = 1.0
          case enableTimes: Int if (enableTimes >= 1000) => featVec(idx + 5 - 1) = 1.0
        }
        idx += 6
        // TODO,
        // postTimes 6
        v.getLong(7).toInt match {
          case postTimes: Int if (postTimes <= 0) => featVec(idx + 0 - 1) = 1.0
          case postTimes: Int if ((postTimes > 0) && (postTimes < 200)) => featVec(idx + 1 - 1) = 1.0
          case postTimes: Int if ((postTimes >= 200) && (postTimes < 500)) => featVec(idx + 2 - 1) = 1.0
          case postTimes: Int if ((postTimes >= 500) && (postTimes < 1000)) => featVec(idx + 3 - 1) = 1.0
          case postTimes: Int if ((postTimes >= 1000) && (postTimes < 2000)) => featVec(idx + 4 - 1) = 1.0
          case postTimes: Int if (postTimes >= 2000) => featVec(idx + 5 - 1) = 1.0
        }
        idx += 6
        // TODO,
        // enableRate 6
        v.getFloat(8).toDouble match {
          case enableRate: Double if (enableRate <= 0) => featVec(idx + 0 - 1) = 1.0
          case enableRate: Double if ((enableRate > 0) && (enableRate < 10)) => featVec(idx + 1 - 1) = 1.0
          case enableRate: Double if ((enableRate >= 10) && (enableRate < 20)) => featVec(idx + 2 - 1) = 1.0
          case enableRate: Double if ((enableRate >= 20) && (enableRate < 50)) => featVec(idx + 3 - 1) = 1.0
          case enableRate: Double if ((enableRate >= 50) && (enableRate < 100)) => featVec(idx + 4 - 1) = 1.0
          case enableRate: Double if (enableRate >= 100) => featVec(idx + 5 - 1) = 1.0
        }
        (userId, featVec)
      }
    ).toJavaRDD.rdd.cache() // better cached
    val users = userFeatures.map(_._1).collect() // collect into master node
    val features = userFeatures.map(_._2).collect() // collect into master node

    var i = 0
    var count = 0
    while (i < users.length) {
      val aUser = users(i)
      val aFeat = features(i)
      var j = i + 1
      var simVec = Map[String, Double]()
      val dbHandle = new Redis(params.redisHost, params.redisPort)
      while (j < users.length) {
        val bUser = users(j)
        val bFeat = features(j)
        val sim = aFeat.dot(bFeat)
        simVec ++= Map(bUser -> sim)
        j += 1
      }
      // get the top20
      val sortedSimVec = simVec.toSeq.sortBy(_._2).slice(simVec.size - 20, simVec.size).toMap
      Try(dbHandle.insertHashRecord(params.redisDB, s"BORADCASTER_SIMILARITY:${aUser}", sortedSimVec)) match {
        case Success(v) => {
          dbHandle.disconnect()
          count += 1
          if (count % 1000 == 0) {
            println(s"Insert broadcaster similarity ${count} records.")
          }
        }
        case Failure(v) => {
          dbHandle.disconnect()
          println("Insert broadcaster similarity failed .")
        }
      }
      i += 1
    }
    sc.stop()
  }
}

package com.sensetime.ad.dm.etl

/**
  * Statistic Feature enable frequency for broadercaster
  *
  * Created by yuanpingzhou on 2/24/17.
  */
object BroadcasterEnableFrequency {

  import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
  import org.apache.spark.sql.hive.HiveContext

  import scala.util.{Failure, Success, Try}
  import scopt.OptionParser

  import com.sensetime.ad.dm.utils.Redis

  case class Parameter(redisHost: String = "10.0.8.81", redisPort: Int = 6379, redisDB: Int = 10,
                       hiveDBName: String = "sara", hiveTableName: String = "ad_broadcaster_show")

  def main(args: Array[String]): Unit = {

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
          | ./bin/spark-submit  --class  com.sensetime.ad.dm.etl.BroadcasterEnableFrequency \
          | recommendation-2.1.3.jar \
          | --redisHost 10.0.8.81 --redisPort 6379 --redisDB 10 --hiveDBName sara --hiveTableName ad_broadcaster_show
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
    val groupedData = sqlContext.sql(s"select concat(t.app_id,'_',t.broadcaster_id) as user_id,t.advertisement_id,count(*) as count from ${params.hiveTableName} t group by t.app_id,t.broadcaster_id,t.advertisement_id")
    groupedData.repartition(groupedData.col("user_id")).foreachPartition(
      iter =>{
        val dbHandle = new Redis(params.redisHost,params.redisPort)
        var freq = Map[String,Map[String,Int]]()
        iter.foreach {
          case r => {
            val bId = r.getString(0)
            val adId = r.getString(1)
            val count = r.getLong(2).toInt
            freq ++= Map(s"BROADCASTER_ENABLE_FREQUENCY:${bId}" -> Map(adId -> count))
          }
        }
        Try(dbHandle.insertMultHashRecord(params.redisDB,freq)) match {
          case Success(v) =>
            println("Insert frequency success .")
            dbHandle.disconnect()
          case Failure(msg) =>
            dbHandle.disconnect()
            println(s"Insert frequency failed : ${msg.getMessage}")
        }
      }
    )
    sc.stop()
  }
}

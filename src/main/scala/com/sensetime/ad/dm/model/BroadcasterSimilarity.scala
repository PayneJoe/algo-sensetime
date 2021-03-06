package com.sensetime.ad.dm.model

/**
  * Compute broadcaster similarity through cosine in cluster mode with resources consumed intensively.
  *
  * Created by yuanpingzhou on 2/22/17.
  */
object BroadcasterSimilarity {

  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry, RowMatrix}
  import org.apache.spark.sql.hive.HiveContext
  import org.apache.spark.mllib.linalg.Vectors

  import scala.util.{Failure, Success, Try}
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
          | ./bin/spark-submit  --class  com.sensetime.ad.dm.model.BroadcasterSimilarity\
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
    val userFeatures = sqlContext.sql(s"select * from ${params.hiveTableName}").map{
      v => {
        val featVec = Array.fill[Double](39)(0)
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
        //Row.fromSeq(Array[Any](userId,featVec.toSeq))
        (userId, Vectors.dense(featVec.toArray))
      }
    }.toJavaRDD.rdd.persist()

    // retrieve users and features
    val users = userFeatures.map(_._1).zipWithIndex().collect().map(v => (v._2,v._1)).toMap // collect users into master node
    val features = userFeatures.map(_._2)

    // ------ transpose matrix
    // * option one
    //    val featMatrix = new RowMatrix(features)
    //    val tFeatMatrix = transposeRowMatrix(featMatrix)
    // * option two
    //convert that RDD to an RDD[IndexedRow]
    val indexedFeature = features.zipWithIndex.map{
      case(value, index) => IndexedRow(index, value)
    }
    //make a matrix
    val indexedFeatMatrix = new IndexedRowMatrix(indexedFeature)
    //calculate the distributions
    val tFeatMatrix = indexedFeatMatrix.toCoordinateMatrix.transpose().toIndexedRowMatrix()
    // ------

    // compute similarity with cosine
    val simMatrix = tFeatMatrix.columnSimilarities().entries.map{
      case MatrixEntry(i, j, u) => ((i, j), u)
    }
    val bUsers = simMatrix.context.broadcast(users)
    simMatrix.foreachPartition(
      partition => {
        val dbHandle = new Redis(params.redisHost,params.redisPort)
        val userMap = bUsers.value
        val block0 = partition.map{
          case row =>{
            val aUser = "BORADCASTER_SIMILARITY:%s".format(userMap.getOrElse(row._1._1,""))
            val bUser = "%s".format(userMap.getOrElse(row._1._2,""))
            ((aUser,bUser),row._2)
          }
        }.toList
        // TODO, can not be re-traversed
        val block1 = partition.map{
          case row =>{
            val aUser = userMap.getOrElse(row._1._1,"")
            val bUser = "BORADCASTER_SIMILARITY:%s".format(userMap.getOrElse(row._1._2,""))
            ((bUser,aUser),row._2)
          }
        }.toList
        val block = block0 ++ block1
        Try(dbHandle.insertMultHashField(params.redisDB,block)) match{
          case Success(v) =>
            dbHandle.disconnect()
            println("Insert similarity success .")
          case Failure(msg) =>
            dbHandle.disconnect()
            println(s"Insert similarity failed : ${msg.getMessage}")
        }
      }
    )
    sc.stop()
  }
}

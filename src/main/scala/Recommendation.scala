package com.sensetime.ad.algo.ranking


/**
  * Created by yuanpingzhou on 1/22/17.
  */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Try,Failure,Success}
import scala.util.Random
import util.control.Breaks._

import breeze.linalg.{DenseVector => BDV}

import com.sensetime.ad.algo.utils._
import com.sensetime.ad.algo.ml._

object Recommendation extends  App{
  private val rand = new Random(5)
  private val host = "localhost"
  private val port = 6379
  private val fixedModelDBNum = 0
  private val randomFeatureDBNum = 2

  private def downSampling(source: Array[String],positiveIdx: Int): Int = {
    var ret = -1

    breakable {
      while (true) {
        val rIdx = rand.nextInt(source.length)
        if (rIdx != positiveIdx) {
          ret = rIdx
          break
        }
      }
    }

    ret
  }

  private def insertModel(timestamp: Long,model: (BDV[Double],Map[String,BDV[Double]]),features: List[String]): Unit ={
    val dbHandle = new Redis(this.host,this.port)

    // insert fixed effect model
    Try(dbHandle.insertSortedRecord(fixedModelDBNum,timestamp,timestamp.toString,model._1,features)) match{
      case Success(v) =>
        println("Insert fixed effect model success .")
      case Failure(msg) =>
        dbHandle.disconnect()
        println(s"Insert fixed effect model failed : ${msg.getMessage}")
        System.exit(1)
    }

    // insert random effect model
    model._2.foreach{
      case (reid,rModel) =>
        Try(dbHandle.insertSortedRecord(randomFeatureDBNum,timestamp,s"${timestamp}_${reid}",rModel,features)) match{
          case Success(v) =>
            println("Insert random effect model success .")
          case Failure(msg) =>
            dbHandle.disconnect()
            println(s"Insert random effect model failed : ${msg.getMessage}")
            System.exit(1)
        }
    }

  }

  if(args.length != 12){
    println(s"params : mode[local|yarn] inputDir outputDir iter alpha0 alpha1 lambda0 lambda1 metric[accuracy|exploss} " +
      s"lossType[log|exp] regularType[l1|l2] method[lbfgs|sgd]")
    System.exit(1)
  }

  val mode = args(0)
  val inputDir = args(1)
  val outputDir = args(2)
  val iter = args(3).toInt
  val alpha0 = args(4).toDouble
  val alpha1 = args(5).toDouble
  val lambda0 = args(6).toDouble
  val lambda1 = args(7).toDouble
  val metric = args(8)
  val metric_aux = "auc"
  val lossType = args(9)
  val regularType = args(10)
  val method = args(11)

  // spark environment
  val conf = new SparkConf().setMaster(mode).setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  // load data
  val sqlContext = new SQLContext(sc)
  val userInfoJson = sqlContext.read.json(s"${inputDir}/broadcast_log_test.txt")
  val clickJson = sqlContext.read.json(s"${inputDir}/broadcaster_ad_show_test.txt")
  val impressionJson = sqlContext.read.json(s"${inputDir}/ad_push_test.txt")
  val joinedRaw = impressionJson.drop("timestamp").join(clickJson.drop("timestamp"),Seq("list_session_id","user_id"),"inner").
                      drop("list_session_id").join(userInfoJson,Seq("user_id"),"inner")
  println(s"columns ${joinedRaw.columns.toList}")

  var randomEffectIdFeat = Set[String]()
  var featureMap = Map[String,Double]()
  val trainRaw = ArrayBuffer[String]()
  val validateRaw = ArrayBuffer[String]()
  println(joinedRaw.count())
  joinedRaw.rdd.foreach{
    case v =>
      val feat_id_list = v.getString(1).split(",")
      val posFeatId = v.getString(2)
      val posIdx = v.getLong(3).toInt
      val ageGroup = v.getLong(4).toInt
      val fansCountLevel = v.getLong(5).toInt
      val gender = v.getLong(6).toInt
      val networkType = v.getLong(7).toInt
      val os = v.getLong(8).toInt
      if(feat_id_list.contains(posFeatId) == true) {

        randomEffectIdFeat ++= feat_id_list
        val negIdx = downSampling(feat_id_list, posIdx)
        // used to join feat info
        // no feat info here
        val negFeatId = feat_id_list(negIdx)
        //
        featureMap ++= Map(s"pos_${posIdx}" -> .0)
        featureMap ++= Map(s"pos_${negIdx}" -> .0)
        featureMap ++= Map(s"age_${ageGroup}" -> .0)
        featureMap ++= Map(s"fans_${fansCountLevel}" -> .0)
        featureMap ++= Map(s"gender_${gender}" -> .0)
        featureMap ++= Map(s"networkType_${networkType}" -> .0)
        featureMap ++= Map(s"os_${os}" -> .0)
        if (rand.nextFloat() > 0.8) {
          trainRaw += s"+1 feat_id:${posFeatId} pos_${posIdx}:1 age_${ageGroup}:1 fans_${fansCountLevel}:1 gender_${gender}:1 networkType_${networkType}:1 os_${os}:1"
          trainRaw += s"-1 feat_id:${negFeatId} pos_${negIdx}:1 age_${ageGroup}:1 fans_${fansCountLevel}:1 gender_${gender}:1 networkType_${networkType}:1 os_${os}:1"
        }
        else {
          validateRaw += s"+1 feat_id:${posFeatId} pos_${posIdx}:1 age_${ageGroup}:1 fans_${fansCountLevel}:1 gender_${gender}:1 networkType_${networkType}:1 os_${os}:1"
          validateRaw += s"-1 feat_id:${negFeatId} pos_${negIdx}:1 age_${ageGroup}:1 fans_${fansCountLevel}:1 gender_${gender}:1 networkType_${networkType}:1 os_${os}:1"
        }
      }
  }
  println(s"Random effect size is ${randomEffectIdFeat.size}")
  println(s"The size of feaure space is ${featureMap.size}")
  val features = featureMap.keys.toList.filter(_ != "feat_id")
  val trainRdd = Data.formatDataWithRandomEffectId(sc.makeRDD(trainRaw),features,"feat_id","log")
  val validateRdd = Data.formatDataWithRandomEffectId(sc.makeRDD(validateRaw),features,"feat_id","log")
  println(s"The size of training data is ${trainRdd.count}")
  println(s"The size of validate data is ${validateRdd.count}")

  val model = GLMM.trainGLMMWithLR(sc,trainRdd,validateRdd,outputDir,featureMap.size,randomEffectIdFeat.toArray,iter,
    (alpha0,alpha1),(lambda0,lambda1),(metric,metric_aux),
    (regularType,regularType),(lossType,lossType),(method,method))

  //println(model._1.slice(0,10))
  //model._2.foreach(v => println(v._1,v._2.slice(0,10)))

  val curTime = System.currentTimeMillis()
  insertModel(curTime,model,features)

}

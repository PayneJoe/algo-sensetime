/**
  * Created by yuanpingzhou on 11/30/16.
  */
package com.sensetime.ad.dm.utils

import scala.collection.mutable

object Data {
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.rdd.RDD
  import breeze.linalg.{Vector => BV, SparseVector => BSV, DenseVector => BDV, DenseMatrix => BDM, _}
  import org.apache.spark.mllib.linalg.{Vectors, Vector => SparkV, SparseVector => SparkSV, DenseVector => SparkDV, Matrix => SparkM}

  /*
  *  format data with random effect id
 */
  def formatDataWithRandomEffectId(data: RDD[String],features: List[String],randomEffectType: String,mode: String): RDD[(Long,(String,LabeledPoint))] = {
    val formated: RDD[(String,LabeledPoint)] = data.map{
      line =>
        val tokens = line.trim.split(" ", -1)
        val label = mode match{
          case "exp" => tokens(0).toDouble
          case "log" if tokens(0).toDouble > 0 => 1.0
          case "log" => 0.0
        }
        //val features: BDV[Double] = BDV.zeros(nFeat)
        val hitSet = mutable.HashSet[String]()
        var randomEffectId0 = "0"
        tokens.slice(1, tokens.length).foreach{
          x =>
            val parts = x.split(":", -1)
            if(parts(0) == randomEffectType){
              randomEffectId0 = parts(1)
            }
            else{
              hitSet.add(parts(0))
            }
        }
        val featureVec = features.map{
          case f if hitSet.contains(f) => 1.0
          case f => 0.0
        }

        (randomEffectId0,LabeledPoint(label,Vectors.dense(featureVec.toArray)))
    }
    val formatedWithIndex = formated.zipWithIndex().map{
      case (lp,index) =>
        (index,lp)
    }
    formatedWithIndex
  }

  /*
  * transform raw data into LablePoint format with index
 */
  def formatData(data: RDD[String],nFeat: Int,mode: String): RDD[(Long,LabeledPoint)] = {
    val formated: RDD[LabeledPoint] = data.map{
      line =>
        val tokens = line.trim.split(" ", -1)
        val label = mode match{
          case "exp" => tokens(0).toDouble
          case "log" if tokens(0).toDouble > 0 => 1.0
          case "log" => 0.0
        }
        val features: BDV[Double] = BDV.zeros(nFeat)
        tokens.slice(1, tokens.length).map {
          x =>
            val hit: Int = x.split(":", -1)(0).toInt
            features.update(hit - 1, 1.toDouble)
        }
        LabeledPoint(label, Vectors.dense(features.toArray))
    }
    val formatedWithIndex = formated.zipWithIndex().map{
      case (lp,index) =>
        (index,lp)
    }
    formatedWithIndex
  }

}

/**
  * Created by yuanpingzhou on 11/30/16.
  */
package com.sensetime.ad.algo.utils

object Data {
  import breeze.linalg.DenseVector
  import org.apache.spark.mllib.linalg.Vectors
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.rdd.RDD
  import breeze.linalg.{Vector => BV, SparseVector => BSV, DenseVector => BDV, DenseMatrix => BDM, _}
  import breeze.numerics.{sqrt,exp,signum,log}
  import org.apache.spark.mllib.linalg.{Vectors, Vector => SparkV, SparseVector => SparkSV, DenseVector => SparkDV, Matrix => SparkM}

  /*
  * transform raw data into LablePoint format with index
 */
  def formatData(data: RDD[String],nFeat: Int): RDD[(Long,LabeledPoint)] = {
    val formated: RDD[LabeledPoint] = data.map{
      line =>
        val tokens = line.trim.split(" ", -1)
        val label = tokens(0).toInt
        var features: BDV[Double] = BDV.zeros(nFeat)
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

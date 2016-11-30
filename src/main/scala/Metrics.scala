/**
  * Created by yuanpingzhou on 11/30/16.
  */
package com.sensetime.ad.algo.utils

object Metrics {
  import breeze.linalg.{Vector => BV, SparseVector => BSV, DenseVector => BDV, DenseMatrix => BDM, _}
  import breeze.numerics.{sqrt,exp,signum,log}
  import org.apache.spark.mllib.linalg.{Vectors, Vector => SparkV, SparseVector => SparkSV, DenseVector => SparkDV, Matrix => SparkM}

  /*
 *  compute auc
 */
  def computeAuc(predict: BDV[Double],groundTruth: BDV[Double]): Double = {

    // retrieve number of positive and negative samples in ground truth
    val nPos = groundTruth.toArray.filter(_>0).length
    val nNeg = groundTruth.toArray.filter(_<=0).length

    // tuple predict with ground truth , and sort with predict
    val pair = predict.toArray.zip(groundTruth.toArray)
    val sortedPair = pair.sortBy(_._1)
    var auc = 0.0.toDouble
    val x = BDV.zeros[Double](predict.length + 1)
    val y = BDV.zeros[Double](predict.length + 1)
    x(0) = 1.0
    y(0) = 1.0

    // calculate auc incrementally
    var i = 1.toInt
    while(i < sortedPair.length) {
      y(i) = (1.0 * sortedPair.slice(i,pair.length).filter(_._2 > 0).length) / nPos
      x(i) = (1.0 * sortedPair.slice(i,pair.length).filter(_._2 <= 0).length) / nNeg
      auc = auc + (((y(i) + y(i - 1))*(x(i - 1) - x(i)))/2.0)
      i += 1
    }
    auc = auc + ((y(i - 1) * x(i - 1))/2.0)

    auc
  }

  /*
  * exponential loss : log(1.0 + exp(-y * predict))
  */
  def expLoss(y_truth: BDV[Double],y_predict: BDV[Double]): Double = {

    val ret = log(1.0 :+ exp(-1.0 :* (y_truth :* y_predict)))

    (ret.sum / ret.length)
  }

}

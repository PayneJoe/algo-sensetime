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
  def computeAuc(groundTruth: BDV[Double],predict: BDV[Double],lossType: String): Double = {

    // retrieve number of positive and negative samples in ground truth
    var nPos: Int = 0
    var nNeg: Int = 0
    if(lossType == "log") {
      nPos = groundTruth.toArray.filter(_ == 1).length
      nNeg = groundTruth.toArray.filter(_ == 0).length
    }
    else if(lossType == "exp"){
      nPos = groundTruth.toArray.filter(_ == 1).length
      nNeg = groundTruth.toArray.filter(_ == -1).length
    }

    // tuple predict with ground truth , and sort with predict
    val pair = predict.toArray.zip(groundTruth.toArray)
    val sortedPair = pair.sortBy(_._1)
    //println(sortedPair.takeRight(100).mkString(","))
    var auc = 0.0.toDouble
    val x = BDV.zeros[Double](predict.length + 1)
    val y = BDV.zeros[Double](predict.length + 1)
    x(0) = 1.0
    y(0) = 1.0

    // calculate auc incrementally
    var i = 1.toInt
    while(i < sortedPair.length) {
      if(lossType == "exp") {
        y(i) = (1.0 * sortedPair.slice(i, pair.length).filter(_._2 > 0).length) / nPos
        x(i) = (1.0 * sortedPair.slice(i, pair.length).filter(_._2 <= 0).length) / nNeg
      }
      else if(lossType == "log"){
        y(i) = (1.0 * sortedPair.slice(i, pair.length).filter(_._2 > 0.5).length) / nPos
        x(i) = (1.0 * sortedPair.slice(i, pair.length).filter(_._2 <= 0.5).length) / nNeg
      }
      auc = auc + (((y(i) + y(i - 1))*(x(i - 1) - x(i)))/2.0)
      i += 1
    }
    auc = auc + ((y(i - 1) * x(i - 1))/2.0)

    auc
  }

  /*
  * compute loss
  *
  * exponential loss : log(1.0 + exp(-y * p))
  * logarithmic loss : -1.0 * (y * log(p) + (1 -y)*log(1 - p))
  */
  def computeLoss(y_truth: BDV[Double],y_predict: BDV[Double],lossType: String): Double = {

    var loss: BDV[Double] = BDV.zeros[Double](y_predict.length)
    if(lossType == "exp") {
      loss = log(1.0 :+ exp(-1.0 :* (y_truth :* y_predict)))
    }
    else if(lossType == "log"){
      loss = -1.0 :* ((y_truth :* log(y_predict)) :+ ((1.0 :- y_truth) :* log(1.0 :- y_predict)))
    }

    (loss.sum / loss.length)
  }

  /*
   * compute accuracy
   */
  def computeAccuracy(y_truth: BDV[Double],y_predict: BDV[Double],lossType: String): Double = {

    var ret: Double = 0.0
    if(lossType == "exp"){
      val truth_sign = signum(y_truth)
      val predict_sign = signum(y_predict)
      ret = (1.0 * (truth_sign :* predict_sign).toArray.filter(_>0).sum) / truth_sign.length
    }
    else if(lossType == "log"){
      val truth = y_truth.toArray
      val predict = y_predict.toArray
      val tupled = predict.zip(truth)
      val count = BDV(tupled.map{
        pair =>
          if(((pair._1 > 0.5) && (pair._2 > 0.5)) || ((pair._1 <= 0.5) && (pair._2 <= 0.5))){
            1.0
          }
          else{
            0.0
          }
      })
      ret = count.sum/predict.length
    }

    ret
  }
}

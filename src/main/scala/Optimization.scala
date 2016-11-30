/**
  * Created by yuanpingzhou on 11/30/16.
  */
package com.sensetime.ad.algo.utils

object Optimization {

  import breeze.linalg.{Vector => BV, SparseVector => BSV, DenseVector => BDV, DenseMatrix => BDM, _}
  import breeze.numerics.{sqrt,exp,signum,log}
  import org.apache.spark.mllib.linalg.{Vectors, Vector => SparkV, SparseVector => SparkSV, DenseVector => SparkDV, Matrix => SparkM}

  /*
  * compute grad with old score
  * loss = log(1.0 + exp(-y * score))
 */
  def computeGradForExpLoss(x: BDV[Double],y: Double,score: Double): BDV[Double] = {

    val oldScore = score
    val enys = exp(-1.0 * y * oldScore)
    val mult = (-1.0 * y * enys) / (1.0 + enys)
    val grad = mult :* x

    grad
  }

  /*
 * gradient descent updater
 */
  def gradientDescentUpdateWithL2(model: BDV[Double],alpha: Double,grad: BDV[Double],lambda: Double,idx: Int): BDV[Double] = {

    val stepSize = alpha/sqrt(idx)
    val regVal = lambda :* model
    val newModel = model :- (stepSize :* (grad :+ regVal))

    newModel
  }

}

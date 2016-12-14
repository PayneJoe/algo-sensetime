/**
  * Created by yuanpingzhou on 11/30/16.
  */
package com.sensetime.ad.algo.utils

import breeze.numerics.abs

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
 * //stepSize(t) = 1.0 / ((1.0 / (1.0 + sqrt(sum_s{1..t-1}{g_s^2}))) - (1.0 / (1.0 + sqrt(sum_s{1..t}{g_s^2})))) + alpha/(sqrt(t))
 */
  def gradientDescentUpdateWithL2(model: BDV[Double],alpha: Double,grad: BDV[Double],lambda: Double,idx: Int,sumGradSqure: BDV[Double]): BDV[Double] = {

    val alpha_1 = 0.003
    val alpha_2 = 1.0
    val alpha_3 = alpha
    //val gradPart = 1.0 / ((alpha_1 :/ (alpha_2 :+ sqrt(sumGradSqure))) - (alpha_1 :/ (alpha_2 :+ sqrt(sumGradSqure :+ (grad :* grad)))))
    //val gradPart = (alpha_3 :+ sqrt(sumGradSqure :+ (grad :* grad)) :- sqrt(sumGradSqure)) :/ alpha_3
    val gradPart = alpha_3 :/ (1.0 :+ sqrt(grad :* grad)) //:/ sqrt(sumGradSqure :+ (grad :* grad))
    val timePart = alpha_3 / sqrt(idx)
    val stepSize = gradPart
    val regVal = lambda :* model
    val newModel = model :- (stepSize :* (grad :+ regVal))

    newModel
  }

  def gradientDescentUpdateWithL1(model: BDV[Double],alpha: Double,grad: BDV[Double],lambda: Double,idx: Int,sumGradSqure: BDV[Double]): BDV[Double] = {

    val alpha_1 = 0.003
    val alpha_2 = 1.0
    val alpha_3 = alpha
    val gradPart = alpha_3 :/ (1.0 :+ sqrt(grad :* grad)) //:/ sqrt(sumGradSqure :+ (grad :* grad))
    val timePart = alpha_3 / sqrt(idx)
    val stepSize = gradPart
    val suggested = model :- (alpha :* grad)
    val prior = alpha :* lambda
    val z = BDV.zeros[Double](model.length)
    val newModel = signum(suggested) :* max(z,(abs(suggested) :- prior))

    newModel
  }

}

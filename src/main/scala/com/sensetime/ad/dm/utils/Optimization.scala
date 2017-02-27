/**
  * Created by yuanpingzhou on 11/30/16.
  */
package com.sensetime.ad.dm.utils

import breeze.numerics.abs

object Optimization {

  import breeze.linalg.{Vector => BV, SparseVector => BSV, DenseVector => BDV, DenseMatrix => BDM, _}
  import breeze.numerics.{sqrt,exp,signum,log}
  import org.apache.spark.mllib.linalg.{Vectors, Vector => SparkV, SparseVector => SparkSV, DenseVector => SparkDV, Matrix => SparkM}

  /*
  * compute gradient
  *
  * exponential loss function is log(1.0 + exp(-y * score)) , gradient of which should be exp(-y * score) * -y * score' / (1 + exp(-y * score)
  * logarithmic loss function is -1.0 * (y * log(p) + (1 - y) * log(1 - p)) , gradient of which should be -(y * p'/p - (1 - y) * p'/(1 - p))
 */
  def computeGradient(x: BDV[Double],y: Double,score: Double,lossType: String): (BDV[Double],Double) = {

    lossType match{
      case "exp" => {
        val enys = exp(-1.0 * y * score)
        val mult = (-1.0 * y * enys) / (1.0 + enys)
        val grad = mult :* x
        val loss = log(1.0 + enys)
        (grad,loss)
      }
      case "log" => {
        val grad = (score - y) :* x
        val loss = -1.0 * (y * log(score) + (1.0 - y) * log(1.0 - score))
        (grad,loss)
      }
      case _ => {
        (BDV.zeros[Double](x.length),.0)
      }
    }
  }

  /*
   * gradient descent updater with L2 regularization
   *
   */
  def gradientDescentUpdate(model: BDV[Double],alpha: Double,grad: BDV[Double],lambda: Double,
                            idx: Int,sumGradSqure: BDV[Double],regular: String): BDV[Double] = {

    val alpha_1 = 0.003
    val alpha_2 = 1.0
    val alpha_3 = alpha
    //val gradPart = 1.0 / ((alpha_1 :/ (alpha_2 :+ sqrt(sumGradSqure))) - (alpha_1 :/ (alpha_2 :+ sqrt(sumGradSqure :+ (grad :* grad)))))
    //val gradPart = (alpha_3 :+ sqrt(sumGradSqure :+ (grad :* grad)) :- sqrt(sumGradSqure)) :/ alpha_3
    val gradPart = alpha_3 :/ (1.0 :+ sqrt(grad :* grad)) //:/ sqrt(sumGradSqure :+ (grad :* grad))
    val timePart = alpha_3 / sqrt(idx)
    val stepSize = timePart

    regular match{
      case "l2" => {
        val regVal = lambda :* model
        model :- (stepSize :* (grad :+ regVal))
      }
      case "l1" => {
        val suggested = model :- (stepSize :* grad)
        val prior = stepSize :* lambda

        val zeroDenseVector = BDV.zeros[Double](model.length)
        signum(suggested) :* max(zeroDenseVector,(abs(suggested) :- prior))
      }
      case _ => BDV.zeros[Double](model.length)
    }
  }
}

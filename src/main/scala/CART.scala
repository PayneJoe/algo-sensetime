package com.sensetime.ad.algo.ml.tree

/**
  * Created by yuanpingzhou on 12/27/16.
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import breeze.linalg.{DenseVector => BDV}

object CART {

  def fit(data: RDD[(Long,LabeledPoint)],residuals: BDV[Double],depth: Int,nFeat: Int): BDV[Double] = {

    val nInstance = residuals.length
    val deltaF = BDV.zeros[Double](nInstance)

    var i = 0
    while(i < depth){
      val lossTotal = BDV.zeros[Double](nFeat)
      val lossLeft = BDV.zeros[Double](nFeat)
      val countTotal = BDV.zeros[Double](nFeat)
      val countLeft = BDV.zeros[Double](nFeat)
      data.foreach{
        case (_,lp) =>
          val x = lp.features.toArray
          val y = lp.label
          var j = 0
          while(j < x.length){
            if(x(j) == 1.0){
              lossLeft(j) += residuals(i)
              countLeft(j) += 1
            }
            lossTotal(j) += residuals(i)
            countTotal(j) += 1
            j += 1
          }
      }
      val bestAvgLoss = (lossTotal :* lossTotal) :/ countTotal
      val suggestedAvgLoss = ((lossLeft :* lossLeft) :/ countLeft) :+ (((lossTotal :- lossLeft) :* (lossTotal :- lossLeft)) :/ (lossTotal - lossLeft))
      val tmp = suggestedAvgLoss.max
      tmp
      i += 1
    }

    deltaF
  }
}

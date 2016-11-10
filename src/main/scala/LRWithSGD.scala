/**
  * paralleled sgd algorithm applied with logic regression
  * @ l1 regularization
  *   learnrate = alpha / sqrt(t)
  *   shrinkage = learnrate * lambda
  *   if abs(w(t)) >  shrinkage , if w(t) > 0 , w(t + 1) = w(t) - shrinkage
  *                               if w(t) < 0 , w(t + 1) = w(t) + shrinkage
  *   if abs(w(t)) < shrinkage , w(t + 1) = 0
  * Created by yuanpingzhou on 10/30/16.
  */

package com.sensetime.ad.algo.ctr

object LRWithSGD {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.mllib.linalg.{Vectors, DenseVector => SparkDV, Matrix => SparkM, SparseVector => SparkSV, Vector => SparkV}
  import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV, _}
  import breeze.math.MutablizingAdaptor.Lambda2
  import breeze.numerics.{abs, log, signum, sqrt,exp}

  import scala.collection.mutable.ArrayBuffer

  def computeAccuracy(predict: Array[Double],y: Array[Double]): Double = {
    val tupled = predict.zip(y)
    val count = BDV(tupled.map{
      pair =>
        if(((pair._1 > 0.5) && (pair._2 > 0.5)) || ((pair._1 <= 0.5) && (pair._2 <= 0.5))){
          1.0
        }
        else{
          0.0
        }
    })
    count.sum/predict.length
  }

  def activate(x: Double): Double = {
    1.0 / (1.0 + exp(-1.0 * x))
  }

  def computePredict(data: RDD[LabeledPoint],w: BDV[Double]): RDD[Double] = {

    val ret = data.map{
      record =>
        val x: BDV[Double] = BDV(record.features.toArray)
        val mult = (x :* w).sum
        activate(mult)
    }

    ret
  }

  def computeLoss(predict: BDV[Double],y: BDV[Double],lambda: Double,w: BDV[Double]): Double ={

    val bias: BDV[Double] = (-y :* log(predict)) :- ((1.0 :- y) :* log(1.0 :- predict))
    val regVal = lambda * (w.map(abs(_)).sum)
    val lossVal = bias.sum/predict.length
    val ret = lossVal + regVal

    ret
  }

  def evaluate(data: RDD[LabeledPoint],w: BDV[Double],lambda: Double,method: String): Double = {
    val y_true = BDV(data.map{lp => lp.label}.collect())
    val y_pred = BDV(computePredict(data, w).collect())
    var ret = 0.0
    if(method == "loss")
      ret = computeLoss(y_pred,y_true,lambda,w)
    else
      ret = computeAccuracy(y_pred.toArray,y_true.toArray)

    ret
  }

  def computeGrad(x: BDV[Double],y: Double,w: BDV[Double]): (BDV[Double],Double) ={
    val mult = (x :* w).sum
    val predict = activate(mult)
    val grad = (predict - y) :* x
    val loss = (-1.0 * y * log(predict)) - ((1.0 - y) * log(1.0 - predict))
    (grad,loss)
  }

  def updater(wold: BDV[Double],grad: BDV[Double],alpha: Double,lambda: Double,ith: Int) : (BDV[Double],Double) = {
    var w = BDV.zeros[Double](wold.length)

    val step = alpha / sqrt(ith.toDouble)
    val shrink = step * lambda
    val w1 = wold :- (step :* grad)
    val len = w.length
    var i = 0
    while(i < len){
      val newW = signum(w1(i)) * max(0.0,abs(w1(i)) - shrink)
      w.update(i,newW)
      i += 1
    }
    val regVal = lambda * (w.map(abs(_)).sum)

    (w,regVal)
  }

  def trainLR_sgd_l1(train: RDD[LabeledPoint],validate: RDD[LabeledPoint],iter: Int,lambda: Double,alpha: Double) = {

    val rand = breeze.stats.distributions.Gaussian(0, 0.1)
    var w: BDV[Double] = BDV.rand(123,rand)
    var regVal = 0.0

    var i = 0
    val metricList = new ArrayBuffer[(Double,Double)](iter)
    while(i < iter){
      println(s"         w : ${w}")
      val wb = train.context.broadcast(w)
      train.repartition(1)
      val (newW,newReg,lossSum,_cnt) = train.mapPartitions {
        partition =>
          // update w with each record in one partition
          var localW = wb.value
          var count = 0
          var localReg = 0.0
          var localLoss = 0.0
          partition.foreach {
            record =>
              //println(localW.toVector)
              val x = BDV(record.features.toArray)
              val y = record.label
              val (grad,loss) = computeGrad(x,y,localW)
              val (_w,regVal) = updater(localW,grad,alpha,lambda,count + 1)
              localW = _w
              localReg = regVal
              localLoss += loss
              count += 1
          }
          Iterator.single((localW,localReg,localLoss,count))
      }.treeReduce{
        // aggregate w with all partitions
        case((w1,r1,l1,c1),(w2,r2,l2,c2)) =>{
          val avgW = ((w1 :* c1.toDouble) :+ (w2 :* c2.toDouble)) :/ (c1 + c2).toDouble
          val avgReg = ((r1 :* c1.toDouble) :+ (r2 * c2.toDouble)) :/ (c1 + c2).toDouble
          (avgW,avgReg,l1 + l2,c1 + c2)
        }
      }

      val p1 = evaluate(train, w,lambda,"accuracy")
      val p2 = evaluate(validate, w,lambda,"accuracy")
      metricList.append((p1,p2))

      w = newW

      i += 1
    }
    metricList.map{
      metric =>
        println(s"loss : [train] ${metric._1} [validate] ${metric._2}")
    }
  }

  def format(data: RDD[String]): (RDD[LabeledPoint]) = {
    val formated: RDD[LabeledPoint] = data.map{
      line =>
        val tokens = line.trim.split(" ", -1)
        var label: Double = 0.0
        if(tokens(0).toDouble > 0)
          label = 1.0
        var features: BDV[Double] = BDV.zeros(123)
        tokens.slice(1, tokens.length).map {
          x =>
            val hit: Int = x.split(":", -1)(0).toInt
            features.update(hit - 1, 1.toDouble)
        }
        LabeledPoint(label,Vectors.dense(features.toArray))
    }
    formated
  }

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setMaster(args(0)).setAppName("LRWithSGD") //.set("spark.executor.memory", "6g")
    val sc = new SparkContext(conf)

    var rawRDD = sc.textFile(args(1))
    println("Total records : " + rawRDD.count)
    val train = format(rawRDD)

    rawRDD = sc.textFile(args(2))
    println("Validate records : " + rawRDD.count())
    val validate = format(rawRDD)

    trainLR_sgd_l1(train,validate,10,0.6,0.0005)
  }
}

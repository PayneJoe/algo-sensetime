/**
  * Created by yuanpingzhou on 11/7/16.
  *
  * ftrl algorithm applied in logic regression
  * @ update function : w(t + 1) = sum(w(1...t))/t - alpha * (sum(grad(1...t))/t + regPara * w(t))
  *   demonstrate that the newer w won't be too far away from previous points and the direction shall not be out of previous directions
  */

package com.sensetime.ad.algo.ctr

object LRWithFTRL {

  import scala.io.Source
  import breeze.linalg.{DenseVector => BDV,DenseMatrix => BDM}
  import scala.collection.mutable.ArrayBuffer
  import breeze.numerics.{exp,sqrt,log,abs}

  def computePredict(w: BDV[Double],x: BDV[Double]): Double ={

    var ret = 0.0
    val expnp = exp(-1.0 * ((w :* x).sum))
    ret = 1.0/(1.0 + expnp)

    ret
  }

  def evaluate(w: BDV[Double],data_x: BDM[Double],data_y: BDV[Double]): Double = {
    var ret = 0.0
    val row = data_x.rows
    var i = 0
    while(i < row){
      val x = data_x(i,::).t
      val y = data_y(i)
      val p = computePredict(w,x)
      if(((p > 0.5)&&(y > 0.5)) || ((p < 0.5) && (y < 0.5)))
        ret += 1.0
      i += 1
    }
    ret/row
  }

  def train_with_ftrl(train_x: BDM[Double],train_y: BDV[Double],valid_x: BDM[Double],valid_y: BDV[Double],
                      alpha: Double,beta: Double,lambda1: Double,lambda2: Double): Unit = {
    val insNum = train_x.rows
    val featNum = train_x.cols
    val w: BDV[Double] = BDV.zeros[Double](featNum)
    val z: BDV[Double] = BDV.zeros[Double](featNum)
    val q: BDV[Double] = BDV.zeros[Double](featNum)

    val metricList: ArrayBuffer[Double] = new ArrayBuffer[Double]()

    var i = 0.toInt
    while(i < insNum){

      val x: BDV[Double] = train_x(i,::).t
      val y: Double = train_y(i)
      val predict = computePredict(w,x)

      var j = 0.toInt
      while(j < featNum){
        val grad = (predict - y)*x(j)
        val sigma = (sqrt(q(j) + grad*grad) - sqrt(q(j))) / alpha
        q(j) += grad * grad
        z(j) += grad - sigma * w(j)
        val eta = alpha / (beta + sqrt(q(j)))
        var sign = -1.0
        if(z(j) > 0.0)
          sign = 1.0
        if(abs(z(j)) < lambda1)
          w(j) = 0.0
        else
          w(j) = (z(j) - lambda1 * sign)/(-1.0 *(lambda2 + eta))
        j += 1
      }
      if((i % 100) == 0) {
        val metric = evaluate(w, valid_x, valid_y)
        metricList.append(metric)
      }
      i += 1
    }

    // compute accuracy of validate data
    metricList.map{
      metric =>
        println(s"metric : ${metric}")
    }
  }

  def loadFile(file: String): (BDM[Double],BDV[Double]) = {
    val x: ArrayBuffer[Double] = new ArrayBuffer[Double]()
    val y: ArrayBuffer[Double] = new ArrayBuffer[Double]()
    var count = 0
    val source = Source.fromFile(file)
    source.getLines.foreach{
      line =>
        val tokens = line.trim.split(" ",-1)
        var label = tokens(0).toDouble
        if(label > 0)
          label = 1.0
        else
          label = 0.0
        val features: BDV[Double] = BDV.zeros[Double](123)
        tokens.slice(1,tokens.length).foreach{
          feat =>
            val hit: Int = feat.split(":", -1)(0).toInt
            features.update(hit - 1, 1.toDouble)
        }
        x ++= features.toArray
        y.append(label)
        count += 1
    }
    val x_mat: BDM[Double] = BDM(x.toArray).reshape(123,count).t
    val y_mat: BDV[Double] = BDV(y.toArray)
    (x_mat,y_mat)
  }

  def main(args: Array[String]): Unit = {

    if(args.length < 2){
      println("para : trainfile validatefile alpha lambda")
      System.exit(1)
    }

    val trainFile = args(0)
    val validatefile = args(1)
    val alpha = args(2).toDouble
    val beta = 1.0
    val lambda1 = args(3).toDouble
    val lambda2 = 0.4

    val (train_x,train_y) = loadFile(trainFile)
    println("train shape : ")
    println(train_x.rows,train_x.cols)

    val (valid_x,valid_y) = loadFile(validatefile)
    println("validate shape : ")
    println(valid_x.rows,valid_x.cols)

    train_with_ftrl(train_x,train_y,valid_x,valid_y,alpha,beta,lambda1,lambda2)

  }
}

/**
  * parallel sgd algorithm applied in factorization machine
  * Created by yuanpingzhou on 10/19/16.
*/
package com.sensetime.ad.algo.ctr

object FMWithSGD {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.rdd.RDD
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.mllib.linalg.{Vectors, Vector => SparkV, SparseVector => SparkSV, DenseVector => SparkDV, Matrix => SparkM}
  import breeze.linalg.{Vector => BV, SparseVector => BSV, DenseVector => BDV, DenseMatrix => BDM, _}

  import breeze.numerics.sqrt
  import breeze.numerics.exp
  import breeze.numerics.log

  import scala.collection.mutable.ArrayBuffer

  implicit def toBreeze(v: SparkV): BV[Double] = {
    /** Convert a spark.mllib.linalg vector into breeze.linalg.vector
      * We use Breeze library for any type of matrix operations because mllib local linear algebra package doesn't have any support for it (in Spark 1.4.1)
      * mllib toBreeze function already exists but is private to mllib scope
      *
      * Automatically choose the representation (dense or sparse) which use the less memory to be store
      */
    val nnz = v.numNonzeros
    if (1.5 * (nnz + 1.0) < v.size) {
      new BSV(v.toSparse.indices, v.toSparse.values, v.size)
    } else {
      BV(v.toArray)
    }

  }

  /*
  * compute predicted value
  * predicted = w0 + w1 * x + sum(w2 * x(i) * x(j))
   */
  def fm_get_p(X: SparkV,W0: Double,W1: BDV[Double], W2: BDM[Double]): Double = {
    val x = X: BV[Double]

    val xa = x.toDenseVector.asDenseMatrix
    val VX = xa * W2
    val VX_square = (xa :* xa) * (W2 :* W2)

    //TODO
    // two-way
    val kth = (VX :* VX) - VX_square
    var phi = kth.sum/2

    // one-way
    phi += (x.toDenseVector :* W1).sum

    //  zero-way
    phi += W0

    phi
  }

  def predictFM(data: RDD[LabeledPoint],W0: Double,W1: BDV[Double], W2: BDM[Double]): RDD[Double] = {
    /** Computes the probabilities given a model for the complete data set */
    data.map(row => fm_get_p(row.features,W0,W1,W2))
  }

  /*
  * lossFunc = log(1 + exp(-y * f(x;w))) + lambda * abs(w)/2
   */
  def computeLoss(predict: BDV[Double],y: BDV[Double],regParams: Array[Double],W0: Double,W1: BDV[Double],W2: BDM[Double]): Double ={

    val regVal0 = regParams(0) * (W0 * W0)/2.0
    val regVal1 = regParams(1) * ((W1 :* W1).sum)/2.0
    val regVal2 = regParams(2) * ((W2 :* W2).sum)/2.0
    val regVal = regVal0 + regVal1 + regVal2

    val lossVal = log(1.0 :+ exp(-1.0 :* (y :* predict)))

    val ret = lossVal.sum/lossVal.length + regVal
    ret
  }

  def evaluate(data: RDD[LabeledPoint], w0: Double,w1: BDV[Double],w2: BDM[Double],regParams: Array[Double],method: String): Double = {
    val y_true = BDV(data.map{lp => lp.label}.collect())
    val y_pred = BDV(predictFM(data, w0,w1,w2).collect())
    //println(y_pred.toArray.toVector)
    var ret = 0.0
    if(method == "loss")
      ret = computeLoss(y_pred,y_true,regParams,w0,w1,w2)
    else
      ret = computeAccuracy(y_pred.toArray,y_true.toArray)
    ret
  }

  def computeAccuracy(predict: Array[Double], truth: Array[Double]): Double = {
    val tupled = predict.zip(truth)
    val hit = BDV(
      tupled.map(
        pair => {
          if (((pair._1.toDouble > 0) && (pair._2.toDouble > 0)) || ((pair._1.toDouble <= 0) && (pair._2.toDouble <= 0)))
            1
          else
            0
        }
      )
    )
    (1.0 * hit.sum) / hit.size
  }

  /*
  * first order descent update
  * w(t+1) = w(t) - step * (grad + regPara * w)
   */
  def updater(alpha: Double,W0: Double,W1: BDV[Double],W2: BDM[Double],regParams: Array[Double],grad0: Double,grad1: BDV[Double],grad2: BDM[Double],ith: Int)
          :(Double,BDV[Double],BDM[Double],Double,Double,Double) = {

    val step = alpha/sqrt(ith.toDouble)
    val newW0 = W0 - (step * (grad0 + (regParams(0) * W0)))
    val regVal0 = regParams(0) * W0 * W0 / 2.0
    //TODO
    // pay attention to expression computing priority
    val newW1 = W1 :- (step * (grad1 :+ (regParams(1) :* W1)))
    val regVal1 = regParams(1) * ((W1 :* W1).sum) / 2.0
    val newW2 = W2 :- (step * (grad2 :+ (regParams(2) :* W2)))
    val regVal2 = regParams(2) * ((W2 :* W2).sum) / 2.0

    (newW0,newW1,newW2,regVal0,regVal1,regVal2)
  }

  def computeGrad(X: SparkV, y: Double, W0: Double,W1: BDV[Double],W2: BDM[Double]): (Double,BDV[Double],BDM[Double],Double) = {
    /* 	Computes the gradient for one instance using Rendle FM paper (2010) trick (linear time computation) */
    val nrFeat = X.size
    val xa = X.toDenseVector.asDenseMatrix

    // mult
    val expnyt = exp(-1.0 :* (y :* fm_get_p(X,W0,W1,W2)))
    val mult = (-y * expnyt)/(1 + expnyt)
    val loss = log(1.0 + expnyt)

    // two way
    val x_matrix = xa.t * xa
    var i = 0
    while (i < nrFeat) {
      x_matrix.update(i, i, 0.0)
      i += 1
    }
    val grad2 = x_matrix * W2
    val result2 = (mult :* grad2)

    // one way
    val result1 = (mult :* X.toDenseVector)

    // zero way
    val result0 = mult

    (result0,result1,result2,loss)
  }

  /*
  * train FM with gradient descent computed with entire training data
  *
   */
  def trainFM_parallel_gd(sc: SparkContext,trainRDD: RDD[LabeledPoint],validRDD: RDD[LabeledPoint], iterations: Int = 50, alpha: Double = 0.01,
                           regParams: Array[Double] = Array(0,0,0.1), factorLength: Int = 4, verbose: Boolean = true): (Double,BDV[Double],BDM[Double]) = {
    var train = trainRDD
    val valid = validRDD
    train.cache()
    valid.cache()

    val rand = breeze.stats.distributions.Gaussian(0, 0.1)
    var W0: Double = 0.0
    var W1: BDV[Double] = BDV.zeros(123)
    var W2: BDM[Double] = BDM.rand(123, factorLength, rand)

    println("train records : %d   valid records : %d ".format(train.count(), valid.count()))

    var i = 1
    val validationList = new ArrayBuffer[(Double,Double)](iterations)
    while(i <= iterations){
      println(W1.toDenseVector)
      //
      val bcWeights0 = train.context.broadcast(W0)
      val bcWeights1 = train.context.broadcast(W1)
      val bcWeights2 = train.context.broadcast(W2)
      train = train.repartition(1)
      //
      val (gradSum0,gradSum1,gradSum2,batchsize) = train.mapPartitions{
        partition =>
          //
          var count: Long = 0L

          val localWeights0 = bcWeights0.value
          val localWeights1 = bcWeights1.value
          val localWeights2 = bcWeights2.value

          var localGradSum0 = 0.0
          var localGradSum1 = BDV.zeros[Double](123)
          var localGradSum2 = BDM.zeros[Double](123,factorLength)
          partition.foreach {
            case (record: LabeledPoint) =>
              val features = record.features
              val label = record.label

              val (_grad0,_grad1,_grad2,loss) = computeGrad(features, label, localWeights0,localWeights1,localWeights2)

              localGradSum0 = localGradSum0 + _grad0
              localGradSum1 = localGradSum1 :+ _grad1
              localGradSum2 = localGradSum2 :+ _grad2

              count += 1L
          }
        Iterator.single((localGradSum0,localGradSum1,localGradSum2,count))
      }.treeReduce{
       case((ga0,ga1,ga2,ca),(gb0,gb1,gb2,cb)) => {
         (ga0 + gb0,ga1 :+ gb1,ga2 :+ gb2,ca + cb)
       }
      }
      val grad0 = gradSum0 / batchsize
      val grad1 = gradSum1 :/ batchsize.toDouble
      val grad2 = gradSum2 :/ batchsize.toDouble

      val (_w0,_w1,_w2,_r0,_r1,_r2) = updater(alpha,W0,W1,W2,regParams,grad0,grad1,grad2,i)
      W0 = _w0
      W1 = _w1
      W2 = _w2

      //
      if(verbose == true) {
        val p1 = evaluate(train, W0,W1,W2,regParams,"loss")
        val p2 = evaluate(valid, W0,W1,W2,regParams,"accuracy")
        validationList.append((p1,p2))
      }
      i += 1
    }
    validationList.map(pair => println(s"train : ${pair._1}  validate : ${pair._2}"))
    (W0,W1,W2)
  }

  /*
  *  train FM with stochastic gradient descent computed with single instance
   */
  def trainFM_parallel_sgd(sc: SparkContext, trainRDD: RDD[LabeledPoint], validRDD: RDD[LabeledPoint],iterations: Int = 50, alpha: Double = 0.01,
                          regParams: Array[Double] = Array(0,0,0.1), factorLength: Int = 4, verbose: Boolean = true):
                          (Double,BDV[Double],BDM[Double]) = {
    var train = trainRDD
    var valid = validRDD
    train.cache()
    valid.cache()

    val rand = breeze.stats.distributions.Gaussian(0, 0.1)
    var W0: Double = 0.0
    var W1: BDV[Double] = BDV.zeros(123)
    var W2: BDM[Double] = BDM.rand(123, factorLength, rand)

    println("train records : %d   valid records : %d ".format(train.count(), valid.count()))

    var i = 1
    val lossHistoryList = new ArrayBuffer[(Double,Double)](iterations)
    train = train.repartition(4)
    while(i <= iterations){
      //
      val bcWeights0 = train.context.broadcast(W0)
      val bcWeights1 = train.context.broadcast(W1)
      val bcWeights2 = train.context.broadcast(W2)

      var regVal = 0.0
      //
      val (newW0,newW1,newW2,_regVal,lossSum,_cnt) = train.mapPartitions{
        partition =>
          // training in one partition
          var count: Long = 0L

          var localWeights0 = bcWeights0.value
          var localWeights1 = bcWeights1.value
          var localWeights2 = bcWeights2.value

          var localReg0 = 0.0
          var localReg1 = 0.0
          var localReg2 = 0.0

          var localLoss = 0.0
          partition.foreach {
            case (record: LabeledPoint) =>
              val features = record.features
              val label = record.label

              val (_grad0,_grad1,_grad2,loss) = computeGrad(features,label,localWeights0,localWeights1,localWeights2)
              val (_w0,_w1,_w2,regVal0,regVal1,regVal2) = updater(alpha,localWeights0,localWeights1,localWeights2,regParams,_grad0,_grad1,_grad2,count.toInt + 1)

              localWeights0 = _w0
              localWeights1 = _w1
              localWeights2 = _w2

              localReg0 = regVal0
              localReg1 = regVal1
              localReg2 = regVal2

              localLoss += loss

              count += 1L
          }
          val localReg = Array(localReg0,localReg1,localReg2)
          Iterator.single((localWeights0,localWeights1,localWeights2,localReg,localLoss,count))
      }.treeReduce{
        // summarize with all partitions
        case((wa0,wa1,wa2,ra,la,ca),(wb0,wb1,wb2,rb,lb,cb)) => {
          val avgW0 = (wa0 * ca.toDouble + wb0 * cb.toDouble)/(ca + cb).toDouble
          val avgW1 = ((wa1 :* ca.toDouble) :+ (wb1 :* cb.toDouble)) :/ (ca + cb).toDouble
          val avgW2 = ((wa2 :* ca.toDouble) :+ (wb2 :* cb.toDouble)) :/ (ca + cb).toDouble

          val numerator = (BDV(ra) :* ca.toDouble) :+ (BDV(rb) :* cb.toDouble)
          val r = (numerator :/ (ca + cb).toDouble).toArray

          (avgW0,avgW1,avgW2,r,la + lb,ca + cb)
        }
      }

      //val loss = (1.0 * lossSum / _cnt) + regVal
      //lossHistoryList.append(loss)
      val p1 = evaluate(train, W0,W1,W2,regParams,"accuracy")
      val p2 = evaluate(valid, W0,W1,W2,regParams,"accuracy")
      lossHistoryList.append((p1,p2))

      W0 = newW0
      W1 = newW1
      W2 = newW2

      //println(W2.toDenseVector)
      regVal = _regVal.sum

      i += 1
    }
    lossHistoryList.map(loss => println(s" train accuracy : ${loss._1}  validate accuracy : ${loss._2}"))

    (W0,W1,W2)
  }

  /*
  * format data with LabelPoint
   */
  def formatData(data: RDD[String]): RDD[LabeledPoint] = {
   val formated: RDD[LabeledPoint] = data.map {
    line =>
     val tokens = line.trim.split(" ", -1)
     val label = tokens(0).toInt
     var features: BDV[Double] = BDV.zeros(123)
     tokens.slice(1, tokens.length).map {
      x =>
       val hit: Int = x.split(":", -1)(0).toInt
       features.update(hit - 1, 1.toDouble)
     }
     LabeledPoint(label, Vectors.dense(features.toArray))
   }
   formated
  }

  def save(sc: SparkContext,w: (Double,BDV[Double],BDM[Double]),outputDir: String): Unit = {
    val w0file = outputDir + "/w0"
    val w1file = outputDir + "/w1"
    val w2file = outputDir + "/w2"

    //
    val wb0 = ArrayBuffer[String]()
    wb0 += s"0:${w._1}"
    val w0rdd = sc.parallelize(wb0)

    var tmpArray = w._2.toArray
    val wb1 = ArrayBuffer[String]()
    var i = 0.toInt
    while(i < tmpArray.length){
      wb1 += s"${i}:${tmpArray(i)}"
      i += 1
    }
    val w1rdd = sc.parallelize(wb1)

    tmpArray = w._3.t.toArray
    val wb2 = ArrayBuffer[String]()
    i = 0.toInt
    while(i < tmpArray.length){
      wb2 += s"${i}:${tmpArray(i)}"
      i += 1
    }
    val w2rdd = sc.parallelize(wb2)

    w0rdd.saveAsTextFile(w0file)
    w1rdd.saveAsTextFile(w1file)
    w2rdd.saveAsTextFile(w2file)

  }

  def main(args: Array[String]) {

    if(args.length != 10){
      println("params : mode[local|yarn-client] trainFile validateFile outputFile iterNum learningRate regParam0 regParam1 regParam2 factorLen")
      System.exit(1)
    }

    val mode = args(0)
    val trainFile = args(1)
    val valideFile = args(2)
    val outputDir = args(3)
    val iterNum = args(4).toInt
    val learningRate = args(5).toDouble
    val regParam0 = args(6).toDouble
    val regParam1 = args(7).toDouble
    val regParam2 = args(8).toDouble
    val factorLen = args(9).toInt

    val conf = new SparkConf().setMaster(mode).setAppName("FMWithSGD") //.set("spark.executor.memory", "6g")
    val sc = new SparkContext(conf)

    var rawRDD = sc.textFile(trainFile)
    println("Total records : " + rawRDD.count)

    val trainRDD = formatData(rawRDD)
    val validRDD = formatData(sc.textFile(valideFile))
    val w = trainFM_parallel_sgd(sc, trainRDD,validRDD, iterNum,learningRate, Array(regParam0,regParam1,regParam2),factorLen, true)

    save(sc,w,outputDir)

  }
}

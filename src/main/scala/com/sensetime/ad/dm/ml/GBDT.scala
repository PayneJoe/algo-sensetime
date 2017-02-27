package com.sensetime.ad.dm.ml

/**
  * Created by yuanpingzhou on 12/27/16.
  */
object GBDT {

  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.rdd.RDD
  import org.apache.spark.mllib.linalg.{Vectors}

  import breeze.numerics.{log,exp,abs}
  import breeze.linalg.{DenseVector => BDV,_}

  import com.sensetime.ad.dm.utils.Data
  import com.sensetime.ad.dm.ml.tree._

  def trainWithGBDT(trainRdd: RDD[(Long,LabeledPoint)],validateRdd: RDD[(Long,LabeledPoint)],iterNum: Int,depth: Int,nFeat: Int) = {

    val train = trainRdd.repartition(1)
    val validate = validateRdd.repartition(1)

    val nTrain = train.count().toInt
    val nValidate = validate.count().toInt
    val nPos = train.filter(v => v._2.label > 0).count()
    val pPos = 1.0 * nPos / nTrain
    val pNeg = 1.0 - pPos
    if(pNeg == 0.0){
      println("Exception : Full negative instances !")
      System.exit(1)
    }
    val initialBias = log(pPos/pNeg)

    // initial bias
    var fTrain = BDV.fill[Double](nTrain,initialBias)
    val fValidate = BDV.fill[Double](nValidate,initialBias)

    //
    val yTrain = BDV(train.map(v => v._2.label).collect())

    var i = 0
    while(i < iterNum){
      val startTime = System.currentTimeMillis()

      // update residuals for each instance
      val residuals = (1.0 :* yTrain) :/ (1.0 :+ exp(yTrain :* fTrain))

      // construct one tree
      val deltaF = CART.fit(train,residuals,depth,nFeat)

      fTrain :+= deltaF

      val endTime = System.currentTimeMillis()
      val timeVal = (endTime - startTime) * 0.001
      println(s"time elapse ${timeVal}(s)")
      i += 1
    }
  }

  def main(args: Array[String]) = {
    if(args.length != 6){
      println("params : mode trainFile validateFile outputDir iterationNum depth")
      System.exit(1)
    }

    val mode = args(0)
    val trainFile = args(1)
    val validateFile = args(2)
    val outputDir = args(3)
    val iterNum = args(4).toInt
    val depth = args(5).toInt

    // spark environment
    val conf = new SparkConf().setMaster(mode).setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // load data
    val rawTrain = sc.textFile(trainFile)
    val rawValidate = sc.textFile(validateFile)
    val trainRdd = Data.formatData(rawTrain,123,"exp")
    val validateRdd = Data.formatData(rawValidate,123,"exp")

    trainWithGBDT(trainRdd,validateRdd,iterNum,depth,123)
  }
}

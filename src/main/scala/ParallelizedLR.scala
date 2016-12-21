/**
  * Paralleled logistic regression with SGD and L-BFGS
  *
  * Created by yuanpingzhou on 10/30/16.
  */

package com.sensetime.ad.algo.ctr

object ParallelizedLR {

  import org.apache.spark.{SparkContext,SparkConf,HashPartitioner}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.mllib.linalg.{Vectors,Vector}

  import breeze.linalg.{ DenseVector => BDV }
  import breeze.numerics.{sqrt,exp,signum,log,abs}
  import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS}

  import com.sensetime.ad.algo.utils._
  import scala.collection.mutable


  /*
   * Compute score for generalized linear regression
   */
  def computeScore(x: BDV[Double],model: BDV[Double],lossType: String): Double = {

    var score = x.dot(model)
    if(lossType == "log"){
      score = activate(score)
    }

    score
  }

  /*
   * Activate with exponential function 1.0 / (1.0 + exp(-score))
   *
   */
  def activate(score: Double): Double = {
    1.0 / (1.0 + exp(-score))
  }

  /*
  *  Performance evaluation
  *
  */
  def evaluate(data: RDD[(Long,LabeledPoint)], model: BDV[Double],metric: (String,String),lossType: String): (Double,Double) = {

    var ret1 = 0.0
    var ret2 = 0.0

    val y_truth = BDV(data.map(x => x._2.label).collect())
    val y_predict = BDV(data.map(x => computeScore(BDV(x._2.features.toArray), model,lossType)).collect())

    if(metric._1 == "accuracy"){
      ret1 = Metrics.computeAccuracy(y_truth,y_predict,lossType)
    }
    else if(metric._1 == "loss"){
      ret1 = Metrics.computeLoss(y_truth,y_predict,lossType)
    }
    if(metric._2 == "auc"){
      ret2 = Metrics.computeAuc(y_truth,y_predict,lossType)
    }

    (ret1,ret2)
  }

  /*
   * Train logistic regression with stochastic gradient descent
   *
   */
  def trainLRWithSGD(trainRdd: RDD[(Long,LabeledPoint)],validate: RDD[(Long,LabeledPoint)],iter: Int,
                     alpha: Double,lambda: Double, metric: String,regularType: String,lossType: String) = {

    val rand = breeze.stats.distributions.Gaussian(0, 0.1)
    var w = BDV.rand[Double](123,rand)

    var i = 0
    val train = trainRdd.repartition(4)

    while(i < iter){
      //println(w)
      val startTime = System.currentTimeMillis()

      val s0 = System.currentTimeMillis()
      val broadcastedModel = train.context.broadcast(w)
      val e0 = System.currentTimeMillis()

      val broadcastedRegularType = train.context.broadcast(regularType)
      val broadcastedLossType = train.context.broadcast(lossType)

      val s1 = System.currentTimeMillis()
      val (newW,_cnt) = train.mapPartitions {
        partition =>
          // update w with sgd in one partition
          var localModel = broadcastedModel.value
          val localRegularType = broadcastedRegularType.value
          val localLossType = broadcastedLossType.value

          var count = 0
          val sumGradSqure = BDV.zeros[Double](localModel.length)
          partition.foreach {
            case (uid,record) =>
              val x = BDV(record.features.toArray)
              val y = record.label
              val score = computeScore(x,localModel,localLossType)
              val (grad,_) = Optimization.computeGradient(x,y,score,lossType)
              localModel = Optimization.gradientDescentUpdate(localModel, alpha, grad, lambda, count + 1, sumGradSqure,localRegularType)
              // TODO
              sumGradSqure :+= (grad :* grad)
              count += 1
          }
          Iterator.single((localModel,count))
      }.treeReduce{
        // aggregate w with all partitions
        case((w1,c1),(w2,c2)) =>{
          val avgW = ((w1 :* c1.toDouble) :+ (w2 :* c2.toDouble)) :/ (c1 + c2).toDouble
          (avgW,c1 + c2)
        }
      }
      broadcastedModel.destroy()
      broadcastedLossType.destroy()
      broadcastedRegularType.destroy()
      val e1 = System.currentTimeMillis()

      val s2 = System.currentTimeMillis()
      val e = evaluate(validate, w,(metric,"auc"),lossType)
      val e2 = System.currentTimeMillis()

      val endTime = System.currentTimeMillis()
      val timeVar = (endTime - startTime) * 0.001
      //println(f"iter ${i} \t metric[${metric}] ${e._1}%.3f \t metric[auc] ${e._2}%.3f \t time elapse ${timeVar}%.3f(s)")

      val s3 = System.currentTimeMillis()
      w = newW
      val e3 = System.currentTimeMillis()

      println(s"t0 ${e0 - s0}\tt1 ${e1 - s1}\tt2 ${e2 - s2}\tt3 ${e3 - s3}")
      i += 1
    }
  }

  /*
   * Compute gradient and loss at a certain (weight) point , average if in distributed situation
   */
  private class CostFun(
                         data: RDD[(Long,LabeledPoint)],
                         regParam: Double,
                         nInstance: Long,
                         regularType: String,
                         lossType: String
                       ) extends DiffFunction[BDV[Double]] {
    override def calculate(model: BDV[Double]): (Double, BDV[Double]) = {

      val nFeat = model.length
      val broadcatedModel = data.context.broadcast(model)
      val broadcastedLossType = data.context.broadcast(lossType)

      val seqOp = (c: (BDV[Double], Double), v: (Long,LabeledPoint)) =>
        (c, v) match {
          case ((grad, loss), (uid, lp)) =>
            val label = lp.label
            val features = BDV(lp.features.toArray)
            val score = computeScore(features,broadcatedModel.value,broadcastedLossType.value)
            val (localGrad,localLoss) = Optimization.computeGradient(features,label,score,broadcastedLossType.value)
            (grad :+ localGrad,loss + localLoss)
        }

      val combOp = (c1: (BDV[Double], Double), c2: (BDV[Double], Double)) =>
        (c1, c2) match { case ((grad1, loss1), (grad2, loss2)) =>
          (grad1 :+ grad2, loss1 + loss2)
        }

      val zeroDenseVector = BDV.zeros[Double](nFeat)
      // if you want to use external variables during transformation , you can achieve that through broadcast ,
      //  otherwise , error "Task not serializable" will occur , more information about this you can check :
      // 1. https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/troubleshooting/javaionotserializableexception.html
      // 2. http://stackoverflow.com/questions/22592811/task-not-serializable-java-io-notserializableexception-when-calling-function-ou
      val (gradientSum, lossSum) = data.treeAggregate((zeroDenseVector, 0.0))(seqOp, combOp)

      // broadcasted model is not needed anymore
      broadcatedModel.destroy()
      broadcastedLossType.destroy()

      var loss = 0.0
      var regLoss = 0.0
      if(regularType == "l2") {
        regLoss = (regParam * (model :* model).sum) / 2.0
      }
      else if(regularType == "l1"){
        regLoss = regParam * abs(model).sum
      }
      loss = (lossSum / nInstance.toDouble) + regLoss

      var grad = BDV.zeros[Double](nFeat)
      var regGrad = BDV.zeros[Double](nFeat)
      if(regularType == "l2") {
        regGrad = regParam * model
      }
      else if(regularType == "l1"){
        regGrad = regParam * signum(model)
      }
      grad = (gradientSum :/ nInstance.toDouble) :+ regGrad

      (loss,grad)
    }
  }

  /*
   * Train logistic regression with L-BFGS authored by Breeze
   */
  def trainLRWithLBFGS(trainRdd: RDD[(Long,LabeledPoint)],validate: RDD[(Long,LabeledPoint)],maxNumIterations: Int = 100,
                       alpha: Double,lambda: Double, metric: String,regularType: String,lossType: String,
                       numCorrections: Int = 7,convergenceTol: Double = 1e-6) = {

    val startTime = System.currentTimeMillis()

    val rand = breeze.stats.distributions.Gaussian(0, 0.1)
    var w = BDV.rand[Double](123, rand)
    val train = trainRdd.repartition(4)

    // initial state
    println(s"Initialized model ${w}")
    val e1 = evaluate(validate, w, (metric, "auc"), lossType)
    println(f"Initialized metric[$metric] ${e1._1}%.3f \t metric[auc] ${e1._2}%.3f")

    val nInstance = train.count()
    val lossHistory = mutable.ArrayBuilder.make[Double]
    val costFun = new CostFun(train, lambda, nInstance, regularType, lossType)
    val lbfgs = new BreezeLBFGS[BDV[Double]](maxNumIterations, numCorrections, convergenceTol)

    // start work
    val states = lbfgs.iterations(new CachedDiffFunction(costFun), w)

    /**
      * NOTE: lossSum and loss is computed using the weights from the previous iteration
      * and regVal is the regularization value computed in the previous iteration as well.
      */
    var state = states.next()
    while (states.hasNext) {
      lossHistory += state.value
      state = states.next()
    }
    lossHistory += state.value
    //println("The last 10 losses "+ lossHistory.result().takeRight(10).mkString(","))
    // update model with last iteration state
    w = state.x
    println(s"Optimized model ${w}")

    val e2 = evaluate(validate, w, (metric, "auc"), lossType)
    println(f"Optimized metric[${metric}] ${e2._1}%.3f \t metric[auc] ${e2._2}%.3f")

    val endTime = System.currentTimeMillis()
    val timeVar = (endTime - startTime) * 0.001
    println(f"Time elapsed ${timeVar}%.3f(s) in total ")
  }

  def main(args: Array[String]): Unit ={
    if(args.length != 10){
      println("param : mode[local|yarn-client] trainFile validateFile iteration " +
                        "alpha lambda metric[exploss|accuracy] loss[exp|log] regular[l1|l2] method")
      System.exit(1)
    }

    val mode = args(0)
    val trainFile = args(1)
    val validateFile = args(2)
    val iterNum = args(3).toInt
    val alpha = args(4).toDouble
    val lambda = args(5).toDouble
    val metricType = args(6)
    val lossType = args(7)
    val regularType = args(8)
    val method = args(9)

    if((lossType != "exp") && (lossType != "log")){
      println("Wrong type of loss function ")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster(mode).setAppName(this.getClass.getName) //.set("spark.executor.memory", "6g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    var rawRDD = sc.textFile(trainFile)
    println("Total records : " + rawRDD.count)
    val train = Data.formatData(rawRDD,123,lossType)

    rawRDD = sc.textFile(validateFile)
    println("Validate records : " + rawRDD.count())
    val validate = Data.formatData(rawRDD,123,lossType)

    if(method == "sgd") {
      trainLRWithSGD(train,validate,iterNum,alpha,lambda,metricType,regularType,lossType)
    }
    else if(method == "lbfgs") {
      trainLRWithLBFGS(train, validate, iterNum, alpha, lambda, metricType, regularType, lossType)
    }
  }
}

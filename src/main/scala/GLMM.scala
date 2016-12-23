package com.sensetime.ad.algo.ctr

/**
  * Created by yuanpingzhou on 11/23/16.
  *
  * Implementation of mixed model including one fixed effect model and one type of random effect model, referring http://www.kdd.org/kdd2016/papers/files/adf0562-zhangA.pdf
  *  authored by linkedin corp.
  */
object GLMM {
  import org.apache.spark.{SparkContext,SparkConf,HashPartitioner}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.mllib.linalg.{Vectors, Vector => SparkV, SparseVector => SparkSV, DenseVector => SparkDV, Matrix => SparkM}

  import breeze.linalg.{Vector => BV, SparseVector => BSV, DenseVector => BDV, DenseMatrix => BDM, _}
  import breeze.numerics.{sqrt,exp,signum,log,abs}
  import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS}

  import scala.collection.mutable.{ArrayBuffer,HashMap}

  import com.sensetime.ad.algo.utils._

  /*
    * performance metrics for mixed model
    *
    * @return two metrics
   */
  def evaluate(data: RDD[(Long,LabeledPoint)],fixedEffectModel: BDV[Double],
               randomEffectType: Int,randomEffectModel: Array[(Int,BDV[Double])],mode: (String,String),lossType: String): (Double,Double) = {

    var ret1 = .0
    var ret2 = .0

    val y_true = BDV(data.map(x => x._2.label).collect())
    val y_predict = BDV(data.map{
      case (uid,lp) =>
        val x = BDV(lp.features.toArray)
        val y = lp.label
        val score = computeScore(x,randomEffectType,fixedEffectModel,randomEffectModel.toMap)
        score
    }.collect())

    if (mode._1 == "loss") {
      ret1 = Metrics.computeLoss(y_true, y_predict,lossType)
    }
    else if(mode._1 == "accuracy"){
      ret1 = Metrics.computeAccuracy(y_true,y_predict,lossType)
    }

    if(mode._2 == "auc"){
      ret2 = Metrics.computeAuc(y_true,y_predict,lossType)
    }

    (ret1,ret2)
  }

  /*
   * logicstic regression activate function
   */
  def activate(score: Double): Double ={
    1.0/(1.0 + exp(-1.0 * score))
  }

  /*
    * update score with biased score
    *
    * @return updated score
    * newScore = oldScore - x * oldModel + x * newModel
   */
  def updateScore(oldScore: Double,x: BDV[Double],oldModel: BDV[Double],newModel: BDV[Double],lossType: String): Double = {

    var newScore = 0.0
    if(lossType == "exp") {
      newScore = oldScore - x.dot(oldModel) + x.dot(newModel)
    }
    else if(lossType == "log"){
      newScore = activate(oldScore - x.dot(oldModel) + x.dot(newModel))
    }

    newScore
  }

  /*
   * update fixed effect model with stochastic greadient descent
   *
   * @return updated model
   */
  def updateFixedEffectModelWithSGD(data: RDD[(Long,(LabeledPoint,Double))],model: BDV[Double],
                                    alpha: Double,lambda: Double,regularType: String,lossType: String): BDV[Double] = {

    val broadcastedModel = data.context.broadcast(model)
    val broadcastedLossType = data.context.broadcast(lossType)
    val broadcastedRegularType = data.context.broadcast(regularType)

    val (newModel,_) = data.mapPartitions{
      partition =>
        val localModel = broadcastedModel.value
        val localLossType = broadcastedLossType.value
        val localRegularType = broadcastedRegularType.value

        var lastLocalModel = localModel.copy
        var newLocalModel = localModel.copy
        var count = 0
        val sumGradSqure = BDV.zeros[Double](localModel.length)
        partition.foreach{
          case (uid,(lp,oldScore)) =>
            val x = BDV(lp.features.toArray)
            val y = lp.label

            val score = updateScore(oldScore,x,localModel,newLocalModel,localLossType)
            val (grad,_) = Optimization.computeGradient(x,y,score,localLossType)
            lastLocalModel = newLocalModel
            newLocalModel = Optimization.gradientDescentUpdate(lastLocalModel,alpha,grad,
              lambda,count + 1,sumGradSqure,localRegularType)

            count += 1
        }
        Iterator.single(newLocalModel,count)
    }.treeReduce{
      case ((m1,c1),(m2,c2)) =>
        val avgModel = ((m1 :* c1.toDouble) :+ (m2 :* c2.toDouble)) :/ (c1 + c2).toDouble
        (avgModel,c1 + c2)
    }
    broadcastedLossType.destroy()
    broadcastedRegularType.destroy()
    broadcastedModel.destroy()

    newModel
  }

  /*
   * update random effect model for specific random effect id locally in a worker node
   * both data and model are in the same node identified by random effect id
   *
   * @return updated random effect model and corresponding scores
   */
  def updateRandomEffectModelWithSGD(data: Iterable[(Long,(LabeledPoint,Double))],oldModel: BDV[Double],
                              alpha: Double,lambda: Double,regularType: String,lossType: String): (BDV[Double],Iterable[(Long,Double)]) = {

    var lastModel = oldModel.copy
    var newModel = oldModel.copy
    var i = 0.toInt
    // go through all samples with certain random effect id
    val sumGradSqureForRandom = BDV.zeros[Double](oldModel.length)
    // notice that data is not a RDD
    data.foreach{
      case (uid,(lp,oldScore)) =>
        val x = BDV(lp.features.toArray)
        val y = lp.label

        val score = updateScore(oldScore,x,oldModel,newModel,lossType)
        val (grad,_) = Optimization.computeGradient(x,y,score,lossType)
        lastModel = newModel
        newModel = Optimization.gradientDescentUpdate(lastModel,alpha,grad,lambda,i + 1,sumGradSqureForRandom,regularType)
        // TODO
        //sumGradSqureForRandom = sumGradSqureForRandom :+ (grad :* grad)

        i += 1
    }

    val s = data.map{
      case (uid,(lp,oldScore)) =>
        val x = BDV(lp.features.toArray)
        val y = lp.label

        val score = updateScore(oldScore,x,oldModel,newModel,lossType)

        (uid,score)
    }

    (newModel,s)
  }

  /*
   * cost function of L-BFGS for random effect
   * parameters in cost function which are external variables will be used in calculate function
   *
   */
  private class CostFunForRandomEffect(
                         data: Iterable[(Long,(LabeledPoint,Double))],initialModel: BDV[Double],nInstance: Long,
                         regParam: Double,regularType: String,lossType: String
                         ) extends DiffFunction[BDV[Double]] {
    override def calculate(model: BDV[Double]): (Double, BDV[Double]) = {

      val nFeat = model.size

      var lossSum = 0.0
      var gradientSum = BDV.zeros[Double](nFeat)
      data.foreach{
        case (uid,(lp,initialScore)) =>
          val x = BDV(lp.features.toArray)
          val y = lp.label

          // update score with biased score computed though initial model
          val score = updateScore(initialScore,x,initialModel,model,lossType)

          val (localGrad,localLoss) = Optimization.computeGradient(x,y,score,lossType)
          gradientSum :+= localGrad
          lossSum += localLoss
      }

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
   * update random effect score locally in a worker node
   * both data and model are in the same node
   *
   */
  def updateRandomEffectScore(data: Iterable[(Long,(LabeledPoint,Double))],newModel: BDV[Double],oldModel: BDV[Double]):
                                      Iterable[(Long,Double)] = {
    // notice that data is not a RDD
    val updatedScore = data.map{
      case (uid,(lp,score)) =>
        val x = BDV(lp.features.toArray)
        val newScore = score - (x :* oldModel).sum  + (x :* newModel).sum
        (uid,newScore)
    }
    updatedScore
  }

  /*
   * update random effect model with L-BFGS authored by the group of Breeze project
   * @return updated model and corresponding scores
   *
   */
  def updateRandomEffectModelWithLBFGS(data: Iterable[(Long,(LabeledPoint,Double))],nInstance: Long, model: BDV[Double],
                                        lambda: Double,regularType: String,lossType: String,
                                        maxNumIterations: Int,numCorrections: Int,convergenceTol: Double):
                                        (BDV[Double],Iterable[(Long,Double)]) = {

    val costFunForRandomEffect = new CostFunForRandomEffect(data,model,nInstance, lambda,regularType,lossType)
    val lbfgsForRandomEffect = new BreezeLBFGS[BDV[Double]](maxNumIterations, numCorrections, convergenceTol)

    // start work
    val states = lbfgsForRandomEffect.iterations(new CachedDiffFunction(costFunForRandomEffect), model)

    var state = states.next()
    while (states.hasNext) {
      state = states.next()
    }
    val newModel = state.x
    val newScore = updateRandomEffectScore(data,newModel,model)

    (newModel,newScore)
  }

  /*
   *  compute effect score including fixed effect and random effect
   *  score = x * fixedEffectModel + x' * randomEffectModel
   *
   */
  def computeScore(x: BDV[Double],randomEffectType: Int,fixedEffectModel: BDV[Double],
                    randomEffectModel: Map[Int,BDV[Double]]): Double = {

    val remainedIndex = ((0 to (randomEffectType - 2)).++((randomEffectType to (x.length - 1))))
    val rex = x(remainedIndex)
    val reid = x(randomEffectType - 1).toInt
    val fixedEffect = (x :* fixedEffectModel).sum
    val randomEffect = (rex :* randomEffectModel.get(reid).get).sum
    val effect = fixedEffect + randomEffect

    effect
  }

  /*
   * cost function for fixed effect
   * parameters in cost function which are external variables will be used in calculate function
   *
   */
  private class CostFunForFixedEffect(
                         data: RDD[(Long,(LabeledPoint,Double))],
                         initialModel: BDV[Double],
                         regParam: Double,
                         nInstance: Long,
                         regularType: String,
                         lossType: String
                       ) extends DiffFunction[BDV[Double]] {
    /*
     * parameter in calculate function get updated while iteration goes on
     */
    override def calculate(model: BDV[Double]): (Double, BDV[Double]) = {

      val nFeat = model.length
      val broadcastedInitialModel = data.context.broadcast(initialModel)
      val broadcatedModel = data.context.broadcast(model)
      val broadcastedLossType = data.context.broadcast(lossType)

      val seqOp = (c: (BDV[Double], Double), v: (Long,(LabeledPoint,Double))) =>
        (c, v) match {
          case ((grad, loss), (uid, ld)) =>
            val lp = ld._1
            val label = lp.label
            val features = BDV(lp.features.toArray)
            val initialScore = ld._2

            // update score with biased score computed through initial model
            val score = updateScore(initialScore,features,broadcastedInitialModel.value,broadcatedModel.value,broadcastedLossType.value)

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

      // notice that data is not a RDD
      val (gradientSum, lossSum) = data.treeAggregate((zeroDenseVector, 0.0))(seqOp, combOp)

      // broadcasted models are not needed anymore
      broadcatedModel.destroy()
      broadcastedLossType.destroy()

      var loss = 0.0
      var regLoss = 0.0
      if(regularType == "l2") {
        regLoss = (regParam * model.dot(model)) / 2.0
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
   * update fixed effect model with L-BFGS authored by group of Breeze project
   * @return updated model
   *
   */
  def updateFixedEffectModelWithLBFGS(
                                       data: RDD[(Long,(LabeledPoint,Double))],nInstance: Long,
                                       model: BDV[Double],lambda: Double,regularType: String,lossType: String,
                                       maxIterNum: Int,numCorrections: Int,convergenceTol: Double): BDV[Double] = {

    val costFunForFixedEffect = new CostFunForFixedEffect(data, model,lambda, nInstance, regularType, lossType)
    val lbfgsForFixedEffect = new BreezeLBFGS[BDV[Double]](maxIterNum, numCorrections, convergenceTol)

    // start work
    val statesForFixedEffect = lbfgsForFixedEffect.iterations(new CachedDiffFunction(costFunForFixedEffect),model)

    var stateForFixedEffect = statesForFixedEffect.next()
    while (statesForFixedEffect.hasNext) {
      stateForFixedEffect = statesForFixedEffect.next()
    }
    val newModel = stateForFixedEffect.x

    newModel
  }


  /*
    * train GLMM with logic regression while GLMM includes one fixed effect model and one random effect model
   */
  def trainGLMMWithLR(sc: SparkContext,trainRdd: RDD[(Long,LabeledPoint)],validateRdd: RDD[(Long,LabeledPoint)],outputDir: String,nFeat: Int,
                      randomEffectType: Int,randomEffectId: Array[Int],iter: Int,alpha: (Double,Double),
                      lambda: (Double,Double),metric: (String,String),regularType: (String,String),lossType: (String,String),method: (String,String),
                      maxLBFGSIterNum: Int = 100,numCorrections: Int = 7,convergenceTol: Double = 1e-6):
                        (BDV[Double],Array[(Int,BDV[Double])]) ={

    // checking point directory
    sc.setCheckpointDir(outputDir)

    val nInstance = trainRdd.count()
    val train = trainRdd

    // initialize fixed effect model with Gaussian
    val rand = breeze.stats.distributions.Gaussian(0, 0.1)
    var fixedEffectModelGlobal = BDV.rand(nFeat,rand)
    val fixedHashPartitioner = new HashPartitioner(4)

    //  initialize random effect model indexed by random effect id
    val randomEffectSize = randomEffectId.length
    val randomEffectModelGlobal = HashMap[Int,BDV[Double]]()
    var k = 0.toInt
    while(k < randomEffectSize){
      randomEffectModelGlobal.put(randomEffectId(k),BDV.rand[Double](nFeat - 1,rand))
      k += 1
    }

    // keep randomEffectModel reside in certain nodes
    val randomEffectPartitioner = new HashPartitioner(randomEffectSize)
    var randomEffectModel = sc.parallelize(randomEffectModelGlobal.toSeq) // need to be sequence type of data , HashMap is not permitted
    randomEffectModel = randomEffectModel.partitionBy(randomEffectPartitioner).persist()

    // broadcast initialized fixed model and random effect model to each nodes , which will save a lot of cost for network I/O
    val broadcastRandomEffectModelGlobal = train.context.broadcast(randomEffectModelGlobal.toMap)
    val broadcastFixedEffectModelGlobal = train.context.broadcast(fixedEffectModelGlobal)
    // initialize score for each record with initial fixed effect model and random effect model
    var scoreGlobal = train.mapPartitions {
      partition =>
        val localRandomEffectModel = broadcastRandomEffectModelGlobal.value // retrieve broadcast data inside of partition
        val localFixedEffectModel = broadcastFixedEffectModelGlobal.value
        partition.map {
          record =>
            val uid = record._1
            val lp = record._2
            val x = BDV(lp.features.toArray)
            val score = computeScore(x, randomEffectType, localFixedEffectModel, localRandomEffectModel)
            (uid, score)
        }
    }.partitionBy(fixedHashPartitioner).persist()
    //broadcastFixedEffectModelGlobal.destroy()
    //broadcastRandomEffectModelGlobal.destroy()

    // locate train data for fixed effect
    val trainForFixedEffect = train.partitionBy(fixedHashPartitioner).persist()

    var i = 0.toInt
    val historyMetrics = ArrayBuffer[Double]()
    while (i < iter) {
      //println(fixedEffectModelGlobal)

      val startTime = System.currentTimeMillis()

      // stage 1 : training data preparation for fixed effect model , both data and score are hashed by uid , and then join them together
      val trainWithScoreForFixedEffect = trainForFixedEffect.join(scoreGlobal)

      // stage 2 : aggregate gradient from each fixed effect partition/node and figure out the new fixed model in master node
      var newFixedEffectModelGlobal = BDV.zeros[Double](fixedEffectModelGlobal.length)
      if(method._1 == "lbfgs") {
        newFixedEffectModelGlobal = updateFixedEffectModelWithLBFGS(trainWithScoreForFixedEffect, nInstance, fixedEffectModelGlobal,
                                              lambda._1, regularType._1, lossType._1, maxLBFGSIterNum, numCorrections, convergenceTol)
      }
      else if(method._1 == "sgd"){
        newFixedEffectModelGlobal = updateFixedEffectModelWithSGD(trainWithScoreForFixedEffect,fixedEffectModelGlobal,alpha._1,lambda._1,
                                             regularType._1,lossType._1)
      }

      // stage 3 : broadcast newly fixed effect model with older fixed effect model back to all fixed effect partitions/nodes
      //           and update score
      // newScore = oldScore - x * oldModel + x * newModel
      val newBroadcastFixedModel = trainWithScoreForFixedEffect.context.broadcast(newFixedEffectModelGlobal)
      val oldBroadcastFixedModel = trainWithScoreForFixedEffect.context.broadcast(fixedEffectModelGlobal)
      val trainWithScoreForRandomEffect = trainWithScoreForFixedEffect.mapPartitions {
        // update score
        partition =>
          val localOldFixedModel = oldBroadcastFixedModel.value // retrieve broadcast data inside partition
          val localNewFixedModel = newBroadcastFixedModel.value
          partition.map {
            case (uid, pair) =>
              val lp = pair._1
              val oldScore = pair._2
              val x = BDV(lp.features.toArray)
              val newScore = oldScore - (x :* localOldFixedModel).sum + (x :* localNewFixedModel).sum

              val y = lp.label
              val remainedIndex = (0 to (randomEffectType - 2)).++((randomEffectType to (x.length - 1)))
              val newx = x(remainedIndex)
              val reid = x(randomEffectType - 1).toInt
              (reid,(uid,(LabeledPoint(y,Vectors.dense(newx.toArray)),newScore)))
          }
      }.partitionBy(randomEffectPartitioner)
      fixedEffectModelGlobal = newFixedEffectModelGlobal //  update fixed effect model in master node

      // stage 4 : training data preparation for random effect model , training data is hashed by random effect type
      //            as random effect model does
      val randomEffectModelWithTrainAndScore = randomEffectModel.cogroup(trainWithScoreForRandomEffect)

      // stage 5 : update random effect model and score for each random effect id locally
      val newRandomEffectModelAndScore = randomEffectModelWithTrainAndScore.flatMapValues{
        case pair =>
          val _model = pair._1.head
          val _data = pair._2
          val _nInstance = _data.size

          var newScore = Iterable[(Long,Double)]()
          var newModel = BDV.zeros[Double](_model.length)

          if(method._2 == "lbfgs") {
            val pair = updateRandomEffectModelWithLBFGS(_data, _nInstance, _model, lambda._2,
              regularType._2, lossType._2, maxLBFGSIterNum, numCorrections, convergenceTol)
            newModel = pair._1
            newScore = pair._2
          }
          else if(method._2 == "sgd"){
            val ret = updateRandomEffectModelWithSGD(_data,_model,alpha._2,lambda._2,regularType._2,lossType._2)
            newModel = ret._1
            newScore = ret._2
          }
          println(newModel.slice(0,5))
          Iterator.single(newModel,newScore)
      }.persist()
      // update random effect model
      // be careful that intermediate RDD which has dependencies has to be persisted after each transformation ,
      // otherwise it will be recomputed when action occurs later then
      randomEffectModel = newRandomEffectModelAndScore.mapValues(v => v._1).partitionBy(randomEffectPartitioner)
      scoreGlobal = newRandomEffectModelAndScore.flatMap(v => v._2._2).partitionBy(fixedHashPartitioner)

      // performance evaluation
      val e = evaluate(validateRdd,fixedEffectModelGlobal,randomEffectType,randomEffectModel.collect(),metric,lossType._1)

      val endTime = System.currentTimeMillis()
      val timeVal = (endTime - startTime) * 0.001
      println(f"iteration ${i}  metric[${metric._1}] ${e._1}%.3f  metric[${metric._2}] ${e._2}%.3f time elapse ${timeVal}%.3f(s)")

      i += 1
    }

    (fixedEffectModelGlobal,randomEffectModel.collect())
  }

  /*
  *  select random effects with sparsity
  *  @return (Int,Double) , the former one is selected random effect , while the latter one is feature size
   */
  def selectRandomEffectWithSparsity(data: RDD[String]): (Int,Int) = {
    val nInstance = data.collect().size
    val count = data.flatMap{
      line =>
        val tokens = line.trim.split(" ",-1)
        val feats = tokens.slice(1,tokens.length).map{
          feature =>
            val pair = feature.split(":")
            (pair(0).toInt, pair(1).toInt)
        }
        feats
    }.reduceByKey((x,y) => (x + y))

    val sparsity = count.map(x => (x._1,1.0 * x._2 / nInstance))
    val minSparsityFeatId = sparsity.takeOrdered(1)(Ordering[Double].reverse.on(_._2))(0)._1
    val maxFeatureId = sparsity.takeOrdered(1)(Ordering[Int].reverse.on(_._1))(0)._1

    (minSparsityFeatId,maxFeatureId)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 13) {
      println("params : Mode[local|yarn] trainFile validateFile OutputDir iteration " +
                        " alpha0 alpha1 lambda0 lambda1 metric[accuracy|exploss] lossType[log|exp] regularType[l1|l2] method[lbfgs|sgd]")
      System.exit(1)
    }

    // parse parameters
    val mode = args(0)
    val trainFile = args(1)
    val validateFile = args(2)
    val outputDir = args(3) + "/model"
    val iter = args(4).toInt
    val alpha0 = args(5).toDouble
    val alpha1 = args(6).toDouble
    val lambda0 = args(7).toDouble
    val lambda1 = args(8).toDouble
    val metric = args(9)
    val metric_aux = "auc"
    val lossType = args(10)
    val regularType = args(11)
    val method = args(12)


    // spark environment
    val conf = new SparkConf().setMaster(mode).setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // load raw data
    val trainRawData = sc.textFile(trainFile)
    val validateRawData = sc.textFile(validateFile)

    // select random effect with sparsity temporarily
    val (randomEffectType, nFeat) = selectRandomEffectWithSparsity(trainRawData)
    println(s"Selected random effect is ${randomEffectType} , the size of feature space is ${nFeat}")

    // transform raw data into LabelPoint format
    val trainRdd = Data.formatData(trainRawData, nFeat,lossType)
    val validateRdd = Data.formatData(validateRawData,nFeat,lossType)

    // there's only two random effect id , as it's encoded with one-hot
    val randomEffectId = Array[Int](0,1)
    //
    val model = trainGLMMWithLR(sc,trainRdd,validateRdd,outputDir,nFeat,randomEffectType,randomEffectId,iter,
                                  (alpha0,alpha1),(lambda0,lambda1),(metric,metric_aux),
                                  (regularType,regularType),(lossType,lossType),(method,method))

  }
}

package com.sensetime.ad.algo.ctr

import scala.collection.mutable

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
  import breeze.numerics.{sqrt,exp,signum,log}
  import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS}

  import scala.collection.mutable.{ArrayBuffer,HashMap}

  import com.sensetime.ad.algo.utils._

  /*
   * compute predict , need to be optimized
   */
  def computeScore(data: RDD[(Long,LabeledPoint)],fixedEffectModel: BDV[Double],randomEffectType: Int,
              randomEffectModel: Array[(Int,BDV[Double])]): RDD[Double] = {

    val _fixedEffectModel = fixedEffectModel
    val _randomEffectModel = BDM.zeros[Double](randomEffectModel.length,randomEffectModel(0)._2.length)
    var i = 0.toInt
    while(i < randomEffectModel.length){
      val x = randomEffectModel(i)._1
      var j = 0.toInt
      while(j < randomEffectModel(i)._2.length) {
        _randomEffectModel.update(x,j,randomEffectModel(i)._2(j))
        j += 1
      }
      i += 1
    }

    val score = data.map{
      case (uid,lp) =>
        val x = BDV(lp.features.toArray)
        val x_1 = x(0 to (randomEffectType - 2))
        val x_2 = x(randomEffectType to x.length - 1)
        val xr = BDV.vertcat(x_1,x_2)
        val reid = x(randomEffectType - 1).toInt
        val y = lp.label
        (x :* _fixedEffectModel).sum + (xr :* _randomEffectModel(reid,::).t).sum
    }

    score
  }


  /*
    * performance metrics for mixed model
   */
  def evaluate(data: RDD[(Long,LabeledPoint)],fixedEffectModel: BDV[Double],
               randomEffectType: Int,randomEffectModel: Array[(Int,BDV[Double])],mode: (String,String),lossType: String): (Double,Double) = {

    var ret1 = .0
    var ret2 = .0

    val y_true = BDV(data.map(x => x._2.label).collect())
    val y_predict = BDV((computeScore(data,fixedEffectModel,randomEffectType,randomEffectModel).collect()))

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
    * aggregate gradients computed in individual partitions
   */
  def aggregateGrad(train: RDD[(Long,(LabeledPoint,Double))],nFeat: Int,lossType: String): (BDV[Double],Int) = {
    val (gradSum,nInstance) = train.mapPartitions{
      partition =>
        var localGradSum = BDV.zeros[Double](nFeat)
        var cnt = 0.toInt
        partition.foreach{
          case (_,(lp,score)) =>
            val y = lp.label
            val x = lp.features
            val s = score
            val (localGrad,_) = Optimization.computeGradient(BDV(x.toArray),y,s,lossType)
            localGradSum = localGradSum :+ localGrad
            cnt += 1.toInt
        }
        Iterator.single(localGradSum,cnt)
    }.treeReduce{
      case ((g0,c0),(g1,c1)) =>
      (g0 :+ g1,c0 + c1)
    }
    (gradSum,nInstance)
  }

  /*
   * update random effect model for specific random effect id
   */
  def updateRandomEffectModelWithSGD(trainRdd: Iterable[(Long,(LabeledPoint,Double))],oldModel: BDV[Double],
                              alpha: Double,lambda: Double,lossType: String): (BDV[Double],Iterable[(Long,Double)]) = {

    var lossList = ArrayBuffer[Double]()

    var model = oldModel
    var i = 0.toInt
    // go through all samples with certain random effect id
    var sumGradSqureForRandom = BDV.zeros[Double](oldModel.length)
    val s = trainRdd.map{
      case (uid,(lp,score)) =>
        val x = BDV(lp.features.toArray)
        val y = lp.label
        val (grad,_) = Optimization.computeGradient(x,y,score,lossType)

        model = Optimization.gradientDescentUpdate(model,alpha,grad,lambda,i + 1,sumGradSqureForRandom,lossType)
        // TODO
        sumGradSqureForRandom = sumGradSqureForRandom :+ (grad :* grad)

        // update new score for each instance
        // s(t + 1) = s - x * r + x * r'
        val newScore = score - (x :* oldModel).sum  + (x :* model).sum
        i += 1

        (uid,newScore)
    }

    //println(lossList.take(100))
    (model,s)
  }

  private class CostFun(
                         data: Iterable[(Long,(LabeledPoint,Double))],
                         regParam: Double
                         ) extends DiffFunction[BDV[Double]] {
    override def calculate(model: BDV[Double]): (Double, BDV[Double]) = {

      val nFeat = model.size
      var nInstance = 0.toInt

      var lossSum = 0.0
      var gradSum = BDV.zeros[Double](nFeat)
      data.foreach{
        case (uid,(lp,score)) =>
          val x = BDV(lp.features.toArray)
          val y = lp.label
          val (localGrad,localLoss) = Optimization.computeGradient(x,y,score,"exp")
          gradSum :+= localGrad
          lossSum += localLoss
          nInstance += 1
      }

      val regLoss = (regParam * (model :* model).sum) / 2.0
      val loss = (lossSum / nInstance.toDouble) + regLoss

      val regGrad = regParam :* model
      val grad = (gradSum :/ nInstance.toDouble) :+ regGrad

      (loss,grad)
    }
  }

    def updateRandomEffectScore(data: Iterable[(Long,(LabeledPoint,Double))],newModel: BDV[Double],oldModel: BDV[Double]):
                                      Iterable[(Long,Double)] = {
      val updatedScore = data.map{
        case (uid,(lp,score)) =>
          val x = BDV(lp.features.toArray)
          val newScore = score - (x :* oldModel).sum  + (x :* newModel).sum
          (uid,newScore)
      }
      updatedScore
    }

    def updateRandomEffectModelWithLBFGS(data: Iterable[(Long,(LabeledPoint,Double))],model: BDV[Double],
                                          lambda: Double,maxNumIterations: Int,numCorrections: Int,convergenceTol: Double):
                                          (BDV[Double],Iterable[(Long,Double)]) = {

      val lossHistory = mutable.ArrayBuilder.make[Double]

      val costFun = new CostFun(data, lambda)
      val lbfgs = new BreezeLBFGS[BDV[Double]](maxNumIterations, numCorrections, convergenceTol)

      val states = lbfgs.iterations(new CachedDiffFunction(costFun), model)

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

      val newModel = state.x
      val newScore = updateRandomEffectScore(data,newModel,model)

      val lossHistoryArray = lossHistory.result()

      //println("LBFGS last 10 loss %s ".format(lossHistoryArray.takeRight(10).mkString(", ")))

      (newModel,newScore)
  }

  /*
   *  compute effect score including fixed effect and random effect
   *  score = x * fixedEffectModel + x' * randomEffectModel
   */
  def computeEffect(x: BDV[Double],randomEffectType: Int,fixedEffectModel: BDV[Double],
                    randomEffectModel: HashMap[Int,BDV[Double]]): Double = {

    val remainedIndex = ((0 to (randomEffectType - 2)).++((randomEffectType to (x.length - 1))))
    val rex = x(remainedIndex)
    val reid = x(randomEffectType - 1).toInt
    val fixedEffect = (x :* fixedEffectModel).sum
    val randomEffect = (rex :* randomEffectModel.get(reid).get).sum
    val effect = fixedEffect + randomEffect

    effect
  }

  /*
    * train GLMM with logic regression while GLMM includes one fixed effect model and one random effect model
   */
  def trainGLMMWithLR(sc: SparkContext,trainRdd: RDD[(Long,LabeledPoint)],validateRdd: RDD[(Long,LabeledPoint)],nFeat: Int,
                      randomEffectType: Int,randomEffectId: Array[Int],iter: Int,alpha: (Double,Double),
                      lambda: (Double,Double),metric: (String,String),lossType: String): (BDV[Double],Array[(Int,BDV[Double])]) ={
    val fixedEffectIterNum = 8.toInt

    var train = trainRdd

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
    val broadcastRandomEffectModelGlobal = train.context.broadcast(randomEffectModelGlobal)
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
            val score = computeEffect(x, randomEffectType, localFixedEffectModel, localRandomEffectModel)
            (uid, score)
        }
    }
    //broadcastFixedEffectModelGlobal.destroy()
    //broadcastRandomEffectModelGlobal.destroy()

    // locate train data for fixed effect
    val trainForFixedEffect = train.partitionBy(fixedHashPartitioner).persist()
    // locate train data for random effect
    /*val trainForRandomEffect = train.map{
      record =>
        val uid = record._1
        val lp = record._2
        val y = lp.label
        val x = BDV(lp.features.toArray)
        val remainedIndex = (0 to (randomEffectType - 2)).++((randomEffectType to (x.length - 1)))
        val newx = x(remainedIndex)
        val reid = x(randomEffectType - 1).toInt
        (reid,(uid,LabeledPoint(y,Vectors.dense(newx.toArray))))
    }.partitionBy(fixedHashPartitioner).persist()
    */

    var i = 0.toInt
    //var sumGradSqureForFixed = BDV.zeros[Double](fixedEffectModelGlobal.length)
    val historyMetrics = ArrayBuffer[Double]()
    while (i < iter) {
      val startTime = System.currentTimeMillis()

      // stage 1 : training data preparation for fixed effect model , both data and score are hashed by uid , and then join them together
      scoreGlobal = scoreGlobal.partitionBy(fixedHashPartitioner)
      var trainWithScoreForFixedEffect = trainForFixedEffect.cogroup(scoreGlobal).mapValues{
        v =>
          (v._1.head,v._2.head)
      }

      var j = 0.toInt
      while(j < fixedEffectIterNum) {
        // stage 2 : aggregate gradient from each fixed effect partition/node and figure out the new fixed model in master node
        /*
        val (gradSum, nInstance) = aggregateGrad(trainWithScoreForFixedEffect, nFeat)
        val grad = gradSum :/ nInstance.toDouble
        val newFixedEffectModelGlobal = Optimization.gradientDescentUpdateWithL1(fixedEffectModelGlobal, alpha._1, grad, lambda._1, i + 1, sumGradSqureForFixed)
        sumGradSqureForFixed = sumGradSqureForFixed :+ (grad :* grad)
        */
        val oldBroadcastFixedModel = train.context.broadcast(fixedEffectModelGlobal)
        val broadcastedLossType = train.context.broadcast(lossType)
        val (newFixedEffectModelGlobal,_) = trainWithScoreForFixedEffect.mapPartitions{
          partition =>
            var localFixedModel = oldBroadcastFixedModel.value
            val localLossType = broadcastedLossType.value
            val localNewFixedModel = BDV.zeros[Double](localFixedModel.length)
            var count = 0.toInt
            val sumGradSqureForFixedEffect = BDV.zeros[Double](localFixedModel.length)
            partition.foreach{
              case (uid,(lp,score)) =>
                val x = BDV(lp.features.toArray)
                val y = lp.label

                val (grad,_) = Optimization.computeGradient(x,y,score,lossType)
                localFixedModel = Optimization.gradientDescentUpdate(localFixedModel,alpha._1,grad,
                                                                      lambda._1,count + 1,sumGradSqureForFixedEffect,localLossType)
                count += 1
            }
            Iterator.single(localNewFixedModel,count)
        }.treeReduce{
          case ((m1,c1),(m2,c2)) =>
            val averagedFixedModel = ((m1 :* c1.toDouble) :+ (m2 :* c2.toDouble)) :/ (c1 + c2).toDouble
            (averagedFixedModel,c1 + c2)
        }
        //oldBroadcastFixedModel.destroy()

        // stage 3 : broadcast newly fixed effect model with older fixed effect model back to all fixed effect partitions/nodes
        //           and update score
        // newScore = oldScore - x * oldModel + x * newModel
        val newBroadcastFixedModel = train.context.broadcast(newFixedEffectModelGlobal)
        fixedEffectModelGlobal = newFixedEffectModelGlobal //  update fixed effect model in master node
        trainWithScoreForFixedEffect = trainWithScoreForFixedEffect.mapPartitions {
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
                (uid, (lp, newScore))
            }
        }
        //newBroadcastFixedModel.destroy()
        j += 1
      }

      // stage 4 : training data preparation for random effect model , training data is hashed by random effect type
      //            as random effect model does
      /*val tmpRdd = trainWithScoreForFixedEffect.map{
        case (uid,(lp,score)) =>
          val x = BDV(lp.features.toArray)
          val reid = x(randomEffectType - 1).toInt
          (reid,(uid,score))
      }.partitionBy(randomEffectPartitioner).cogroup(trainForRandomEffect).flatMapValues {
        case (v2, v1) =>
          val xy = v1.toMap
          val score = v2.toMap
          val buffer = new ArrayBuffer[(Long, (LabeledPoint, Double))]()
          xy.foreach {
            case (uid, lp) =>
              buffer.append((uid, (lp, score.get(uid).head)))
          }
          buffer
      }*/
      val tmpRdd = trainWithScoreForFixedEffect.map{
        case (uid,(lp,score)) =>
          val y = lp.label
          val x = BDV(lp.features.toArray)
          val remainedIndex = (0 to (randomEffectType - 2)).++((randomEffectType to (x.length - 1)))
          val newx = x(remainedIndex)
          val reid = x(randomEffectType - 1).toInt
          (reid,(uid,(LabeledPoint(y,Vectors.dense(newx.toArray)),score)))
      }
      val trainWithScoreForRandomEffect = tmpRdd.partitionBy(randomEffectPartitioner)
      val randomEffectModelWithTrainAndScore = randomEffectModel.cogroup(trainWithScoreForRandomEffect)

      // stage 5 : update random effect model and score for each random effect id locally
      val newRandomEffectModelAndScore = randomEffectModelWithTrainAndScore.flatMapValues{
        case pair =>
          val _model = pair._1.head
          val _data = pair._2

          val (newModel,newScore) = updateRandomEffectModelWithSGD(_data,_model,alpha._2,lambda._2,lossType)
          //val (newModel,newScore) = updateRandomEffectModelWithLBFGS(_data,_model,lambda._2,200,8,1e-4)

          Iterator.single(newModel,newScore)
      }
      randomEffectModel = newRandomEffectModelAndScore.mapValues(v => v._1)
      scoreGlobal = newRandomEffectModelAndScore.flatMap(v => v._2._2)

      val endTime = System.currentTimeMillis()
      val timeVal = (endTime - startTime) * 0.001

      // evaluate metrics including exploss , accuracy , auc
      val e = evaluate(validateRdd,fixedEffectModelGlobal,randomEffectType,randomEffectModel.collect(),metric,lossType)
      println(f"iteration ${i}  metric[${metric._1}] ${e._1}%.3f  metric[${metric._2}] ${e._2}%.3f time elapse ${timeVal}%.3f(s)")
      historyMetrics.append(e._1)

      i += 1
    }

    println(historyMetrics.take(iter))
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
    if (args.length < 11) {
      println("params : Mode[local|yarn] trainFile validateFile OutputDir iteration " +
                        " alpha0 alpha1 lambda0 lambda1 metric[accuracy|exploss] lossType[log|exp]")
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
    val lossType = "exp"

    // spark environment
    val conf = new SparkConf().setMaster(mode).setAppName("GLMM")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // load raw data
    val trainRawData = sc.textFile(trainFile)
    val validateRawData = sc.textFile(validateFile)

    // select random effect with sparsity
    val (randomEffectType, nFeat) = selectRandomEffectWithSparsity(trainRawData)
    println(s"Selected random effect is ${randomEffectType} , the size of feature space is ${nFeat}")

    // transform raw data into LabelPoint format
    val trainRdd = Data.formatData(trainRawData, nFeat,lossType)
    val validateRdd = Data.formatData(validateRawData,nFeat,lossType)
    //val ins = trainRdd.lookup(3)(0)
    //println(ins.label, ins.features)

    // there's only two random effect id ,as it's encoded with one-hot
    val randomEffectId = Array[Int](0,1)
    //
    val model = trainGLMMWithLR(sc,trainRdd,validateRdd,nFeat,randomEffectType,randomEffectId,iter,
                                  (alpha0,alpha1),(lambda0,lambda1),(metric,metric_aux),lossType)

  }
}

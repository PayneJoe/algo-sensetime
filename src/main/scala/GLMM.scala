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
  import breeze.numerics.{sqrt,exp,signum,log}

  import scala.collection.mutable.{ArrayBuffer,HashMap}

  /*
    * compute grad with old score
    * loss = log(1.0 + exp(-y * score))
   */
  def computeGrad(x: BDV[Double],y: Double,score: Double): BDV[Double] = {

    val oldScore = score
    val enys = exp(-1.0 * y * oldScore)
    val mult = (-1.0 * y * enys) / (1.0 + enys)
    val grad = mult :* x

    grad
  }

  /*
    * gradient descent update : w(t + 1) = w(t) - alpha * (grad + reg)
   */
  def updateFixedEffectModel(oldModel: BDV[Double],lr: Double,t: Int,grad: BDV[Double],lambda: Double): BDV[Double] = {

    // regular part
    val regVal = lambda :* oldModel
    // step size
    val stepSize = lr/sqrt(1.0 * t)
    // gradient descent
    val newModel = oldModel :- (stepSize :* (grad :+ regVal))

    newModel
  }

  /*
  * exponential loss : log(1.0 + exp(-y * predict))
   */
  def expLoss(y_truth: BDV[Double],y_predict: BDV[Double]): Double = {

    val ret = log(1.0 :+ exp(-1.0 :* (y_truth :* y_predict)))

    (ret.sum / ret.length)
  }

  /*
   * compute predict , need to be optimized
   */
  def predict(data: RDD[(Long,LabeledPoint)],fixedEffectModel: BDV[Double],randomEffectType: Int,
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
   *  compute auc
   */
  def computeAuc(predict: BDV[Double],groundTruth: BDV[Double]): Double = {

    // retrieve number of positive and negative samples in ground truth
    val nPos = groundTruth.toArray.filter(_>0).length
    val nNeg = groundTruth.toArray.filter(_<=0).length

    // tuple predict with ground truth , and sort with predict
    val pair = predict.toArray.zip(groundTruth.toArray)
    val sortedPair = pair.sortBy(_._1)
    var auc = 0.0.toDouble
    val x = BDV.zeros[Double](predict.length + 1)
    val y = BDV.zeros[Double](predict.length + 1)
    x(0) = 1.0
    y(0) = 1.0

    // calculate auc incrementally
    var i = 1.toInt
    while(i < sortedPair.length) {
      y(i) = (1.0 * sortedPair.slice(i,pair.length).filter(_._2 > 0).length) / nPos
      x(i) = (1.0 * sortedPair.slice(i,pair.length).filter(_._2 <= 0).length) / nNeg
      auc = auc + (((y(i) + y(i - 1))*(x(i - 1) - x(i)))/2.0)
      i += 1
    }
    auc = auc + ((y(i - 1) * x(i - 1))/2.0)

    auc
  }

  /*
    * performance metrics for mixed model
   */
  def evaluate(data: RDD[(Long,LabeledPoint)],fixedEffectModel: BDV[Double],
               randomEffectType: Int,randomEffectModel: Array[(Int,BDV[Double])],mode: String): Double = {

    val y_true = BDV(data.map(x => x._2.label).collect())
    val y_predict = BDV((predict(data,fixedEffectModel,randomEffectType,randomEffectModel).collect()))
    //println((1.0 * y_predict.toArray.filter(_>0).length)/y_predict.length)
    var ret = 0.0.toDouble
    if (mode == "exploss") {
      ret = expLoss(y_true, y_predict)
    }
    else if(mode == "accuracy"){
      val y_sign = signum(y_predict)
      ret = (y_sign :* y_true).toArray.filter(_>0).sum / y_true.length
    }
    else if(mode == "auc"){
      ret = computeAuc(y_predict,y_true)
    }

    ret
  }

  /*
    * aggregate gradients computed in individual partitions
   */
  def aggregateGrad(train: RDD[(Long,(LabeledPoint,Double))],nFeat: Int): (BDV[Double],Int) = {
    val (gradSum,nInstance) = train.mapPartitions{
      partition =>
        var localGradSum = BDV.zeros[Double](nFeat)
        var cnt = 0.toInt
        partition.foreach{
          case (_,(lp,score)) =>
            val y = lp.label
            val x = lp.features
            val s = score
            localGradSum = localGradSum :+ computeGrad(BDV(x.toArray),y,s)
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
   * gradient descent updater
   */
  def updater(model: BDV[Double],alpha: Double,grad: BDV[Double],lambda: Double,idx: Int): BDV[Double] = {

    val stepSize = alpha/sqrt(idx)
    val regVal = lambda :* model
    val newModel = model :- (stepSize :* (grad :+ regVal))

    newModel
  }

  /*
   * update random effect model for specific random effect id
   */
  def updateRandomEffectModel(trainRdd: Iterable[(Long,(LabeledPoint,Double))],oldModel: BDV[Double],
                              alpha: Double,lambda: Double): (BDV[Double],Iterable[(Long,Double)]) = {

    var lossList = ArrayBuffer[Double]()

    var model = oldModel
    var i = 0.toInt
    // go through all samples with certain random effect id
    val s = trainRdd.map{
      case (uid,(lp,score)) =>
        val x = BDV(lp.features.toArray)
        val y = lp.label
        val grad = computeGrad(x,y,score)

        model = updater(model,alpha,grad,lambda,i + 1)

        // update new score for each instance
        // s(t + 1) = s - x * r + x * r'
        val newScore = score - (x :* oldModel).sum  + (x :* model).sum
        i += 1

        (uid,newScore)
    }

    //println(lossList.take(100))
    (model,s)
  }

  /*
   *  compute effect score including fixed effect and random effect
   *  score = x * fixedEffectModel + x' * randomEffectModel
   */
  def computeEffect(x: BDV[Double],randomEffectType: Int,fixedEffectModel: BDV[Double],
                    randomEffectModel: HashMap[Int,BDV[Double]]): Double = {

    val x_1 = x(0 to (randomEffectType - 2))
    val x_2 = x(randomEffectType to x.length - 1)
    val rex = BDV.vertcat(x_1,x_2)
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
                      lambda: (Double,Double)): (BDV[Double],Array[(Int,BDV[Double])]) ={

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
    var randomEffectModel = sc.parallelize(randomEffectModelGlobal.toSeq)
    randomEffectModel = randomEffectModel.partitionBy(randomEffectPartitioner).persist()

    // broadcast initialized fixed model and random effect model to save cost of network I/O
    val broadcastRandomEffectModelGlobal = train.context.broadcast(randomEffectModelGlobal)
    val broadcastFixedEffectModelGlobal = train.context.broadcast(fixedEffectModelGlobal)
    // initialize score for each record
    var scoreGlobal = train.mapPartitions {
      partition =>
        val localRandomEffectModel = broadcastRandomEffectModelGlobal.value
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

    var i = 0.toInt
    while (i < iter) {

      // stage 1 : preparation , repartition data and score, and then join them together for next move
      train = train.partitionBy(fixedHashPartitioner)
      scoreGlobal = scoreGlobal.partitionBy(fixedHashPartitioner)
      var trainWithScore = train.cogroup(scoreGlobal).mapValues{
        v =>
          (v._1.head,v._2.head)
      }

      // stage 2 : aggregate gradient from each fixed effect partition/node and figure out the new fixed model in master node
      val (gradSum,nInstance) = aggregateGrad(trainWithScore,nFeat)
      val grad = gradSum :/ nInstance.toDouble
      val newFixedEffectModelGlobal = updateFixedEffectModel(fixedEffectModelGlobal,alpha._1,i + 1,grad,lambda._1)

      // stage 3 : broadcast newly fixed effect model with older fixed effect model back to all fixed effect partitions/nodes
      //           and update score
      // newScore = oldScore - x * oldModel + x * newModel
      val oldBroadcastFixedModel = train.context.broadcast(fixedEffectModelGlobal)
      val newBroadcastFixedModel = train.context.broadcast(newFixedEffectModelGlobal)
      fixedEffectModelGlobal = newFixedEffectModelGlobal  //  update fixed effect model in master node
      trainWithScore = trainWithScore.mapPartitions{  // update score
        partition =>
              val localOldFixedModel = oldBroadcastFixedModel.value
              val localNewFixedModel = newBroadcastFixedModel.value
              partition.map{
                case (uid,pair) =>
                  val lp = pair._1
                  val oldScore = pair._2
                  val x = BDV(lp.features.toArray)
                  val newScore = oldScore - (x :* localOldFixedModel).sum + (x :* localNewFixedModel).sum
                  (uid,(lp,newScore))
              }
      }

      // stage 4 : preparation , training data for random effect
      val tmpRdd = trainWithScore.map{
        record =>
          val uid = record._1
          val lp = record._2._1
          val score = record._2._2
          val y = lp.label
          val x = BDV(lp.features.toArray)
          val x_1 = x(0 to (randomEffectType - 2))
          val x_2 = x(randomEffectType to x.length - 1)
          val newx = BDV.vertcat(x_1,x_2)
          val reid = x(randomEffectType - 1).toInt
          (reid,(uid,(LabeledPoint(y,Vectors.dense(newx.toArray)),score)))
      }
      val trainForRandomEffectWithScore = tmpRdd.partitionBy(randomEffectPartitioner)
      val randomEffectModelWithTrainAndScore = randomEffectModel.cogroup(trainForRandomEffectWithScore)

      // stage 5 : update random effect model and score for each random effect id locally
      val newRandomEffectModelAndScore = randomEffectModelWithTrainAndScore.flatMapValues{
        case pair =>
          val _model = pair._1.head
          val _data = pair._2

          val (newModel,newScore) = updateRandomEffectModel(_data,_model,alpha._2,lambda._2)

          Iterator.single(newModel,newScore)
      }
      randomEffectModel = newRandomEffectModelAndScore.mapValues(v => v._1)
      scoreGlobal = newRandomEffectModelAndScore.flatMap(v => v._2._2)

      // evaluate
      val metric = evaluate(validateRdd,fixedEffectModelGlobal,randomEffectType,randomEffectModel.collect(),"auc")
      //println(fixedEffectModelGlobal.slice(0,20))
      //println(randomEffectModel.collect.slice(0,20).toVector)
      println(s"iteration ${i}   metric[AUC] : ${metric}")

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

  /*
    * transform raw data into LablePoint format with index
   */
  def formatData(data: RDD[String],nFeat: Int): RDD[(Long,LabeledPoint)] = {
    val formated: RDD[LabeledPoint] = data.map{
      line =>
        val tokens = line.trim.split(" ", -1)
        val label = tokens(0).toInt
        var features: BDV[Double] = BDV.zeros(nFeat)
        tokens.slice(1, tokens.length).map {
          x =>
            val hit: Int = x.split(":", -1)(0).toInt
            features.update(hit - 1, 1.toDouble)
        }
        LabeledPoint(label, Vectors.dense(features.toArray))
    }
    val formatedWithIndex = formated.zipWithIndex().map{
      case (lp,index) =>
        (index,lp)
    }
    formatedWithIndex
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      println("params : Mode[local|yarn] trainFile validateFile OutputDir iteration alpha0 alpha1 lambda0 lambda1")
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
    val trainRdd = formatData(trainRawData, nFeat)
    val validateRdd = formatData(validateRawData,nFeat)
    //val ins = trainRdd.lookup(3)(0)
    //println(ins.label, ins.features)

    // there's only two random effect id ,as it's encoded with one-hot
    val randomEffectId = Array[Int](0,1)
    //
    val model = trainGLMMWithLR(sc,trainRdd,validateRdd,nFeat,randomEffectType,randomEffectId,iter,(alpha0,alpha1),(lambda0,lambda1))

  }
}

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

  import com.sensetime.ad.algo.utils._

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
    * performance metrics for mixed model
   */
  def evaluate(data: RDD[(Long,LabeledPoint)],fixedEffectModel: BDV[Double],
               randomEffectType: Int,randomEffectModel: Array[(Int,BDV[Double])],mode: String): Double = {

    val y_true = BDV(data.map(x => x._2.label).collect())
    val y_predict = BDV((predict(data,fixedEffectModel,randomEffectType,randomEffectModel).collect()))
    var ret = 0.0.toDouble
    if (mode == "exploss") {
      ret = Metrics.expLoss(y_true, y_predict)
    }
    else if(mode == "accuracy"){
      val y_sign = signum(y_predict)
      ret = (y_sign :* y_true).toArray.filter(_>0).sum / y_true.length
    }
    else if(mode == "auc"){
      ret = Metrics.computeAuc(y_predict,y_true)
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
            localGradSum = localGradSum :+ Optimization.computeGradForExpLoss(BDV(x.toArray),y,s)
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
        val grad = Optimization.computeGradForExpLoss(x,y,score)

        model = Optimization.gradientDescentUpdateWithL2(model,alpha,grad,lambda,i + 1)

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
                      lambda: (Double,Double),metric: String): (BDV[Double],Array[(Int,BDV[Double])]) ={

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

    var i = 0.toInt
    while (i < iter) {

      // stage 1 : training data preparation for fixed effect model , both data and score are hashed by uid , and then join them together
      train = train.partitionBy(fixedHashPartitioner)
      scoreGlobal = scoreGlobal.partitionBy(fixedHashPartitioner)
      var trainWithScore = train.cogroup(scoreGlobal).mapValues{
        v =>
          (v._1.head,v._2.head)
      }

      // stage 2 : aggregate gradient from each fixed effect partition/node and figure out the new fixed model in master node
      val (gradSum,nInstance) = aggregateGrad(trainWithScore,nFeat)
      val grad = gradSum :/ nInstance.toDouble
      val newFixedEffectModelGlobal = Optimization.gradientDescentUpdateWithL2(fixedEffectModelGlobal,alpha._1,grad,lambda._1,i + 1)

      // stage 3 : broadcast newly fixed effect model with older fixed effect model back to all fixed effect partitions/nodes
      //           and update score
      // newScore = oldScore - x * oldModel + x * newModel
      val oldBroadcastFixedModel = train.context.broadcast(fixedEffectModelGlobal)
      val newBroadcastFixedModel = train.context.broadcast(newFixedEffectModelGlobal)
      fixedEffectModelGlobal = newFixedEffectModelGlobal  //  update fixed effect model in master node
      trainWithScore = trainWithScore.mapPartitions{  // update score
        partition =>
              val localOldFixedModel = oldBroadcastFixedModel.value // retrieve broadcast data inside partition
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

      // stage 4 : training data preparation for random effect model , training data is hashed by random effect type
      //            as random effect model does
      val tmpRdd = trainWithScore.map{
        record =>
          val uid = record._1
          val lp = record._2._1
          val score = record._2._2
          val y = lp.label
          val x = BDV(lp.features.toArray)
          /*val x_1 = x(0 to (randomEffectType - 2))
          val x_2 = x(randomEffectType to x.length - 1)
          val newx = BDV.vertcat(x_1,x_2)*/
          val remainedIndex = (0 to (randomEffectType - 2)).++((randomEffectType to (x.length - 1)))
          val newx = x(remainedIndex)
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

      // evaluate metrics including exploss , accuracy , auc
      val e = evaluate(validateRdd,fixedEffectModelGlobal,randomEffectType,randomEffectModel.collect(),metric)
      println(s"iteration ${i}   metric[accuracy] : ${e}")

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
    if (args.length < 10) {
      println("params : Mode[local|yarn] trainFile validateFile OutputDir iteration alpha0 alpha1 lambda0 lambda1 metric[accuracy|exploss|auc]")
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
    val trainRdd = Data.formatData(trainRawData, nFeat)
    val validateRdd = Data.formatData(validateRawData,nFeat)
    //val ins = trainRdd.lookup(3)(0)
    //println(ins.label, ins.features)

    // there's only two random effect id ,as it's encoded with one-hot
    val randomEffectId = Array[Int](0,1)
    //
    val model = trainGLMMWithLR(sc,trainRdd,validateRdd,nFeat,randomEffectType,randomEffectId,iter,(alpha0,alpha1),(lambda0,lambda1),metric)

  }
}

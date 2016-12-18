/**
  * Created by yuanpingzhou on 11/8/16.
  *
  * This is implementation of FTRL-PROXIMAL algorithm based factorization machine , referring http://www.jmlr.org/proceedings/papers/v15/mcmahan11b/mcmahan11b.pdf
  * authored by google corp.
  */
package com.sensetime.ad.algo.ctr

object FMWithFTRL {

  import scala.io.Source
  import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV, norm => brzNorm, _}
  import scala.collection.mutable.ArrayBuffer
  import breeze.numerics.{exp,sqrt,log,abs}
  import breeze.numerics.signum

  import java.io._

  import com.sensetime.ad.algo.utils._

  def computePredict(x: BDV[Double],w: (Double,BDV[Double],BDM[Double])): Double ={

    val xa = x.toDenseVector.asDenseMatrix
    val VX = xa * w._3
    val VX_square = (xa :* xa) * (w._3 :* w._3)

    //TODO
    // two-way
    val kth = (VX :* VX) - VX_square
    var phi = kth.sum/2

    // one-way
    phi += (x.toDenseVector :* w._2).sum

    //  zero-way
    phi += w._1

    phi
  }

  def computeAccuracy(predict: BDV[Double],y: BDV[Double]): Double = {
    val k = signum(predict) :* signum(y)
    var hit = 0.toInt
    k.map{
      v =>
        if(v > 0)
          hit += 1
    }
    (1.0 * hit) / predict.length
  }

  def evaluate(w: (Double,BDV[Double],BDM[Double]),data_x: BDM[Double],data_y: BDV[Double],mode: String): Double = {

    val rows = data_x.rows
    val cols = data_x.cols
    val predict = BDV.zeros[Double](rows)
    var i = 0.toInt
    while(i < rows){
      val x = data_x(i,::).t
      val p = computePredict(x,w)
      predict.update(i,p)
      i += 1
    }

    var ret = 0.0
    if(mode == "accuracy") {
      ret = computeAccuracy(predict, data_y)
    }
    else if(mode == "auc"){
      ret = Metrics.computeAuc(predict,data_y,"exp")
    }
    ret
  }

  def computeGrad(x: BDV[Double],y: Double,w: (Double,BDV[Double],BDM[Double])): ((Double,BDV[Double],BDM[Double]),Double,Double) = {
    //
    val nrFeat = x.size
    val xa = x.toDenseVector.asDenseMatrix

    // mult
    val predict = computePredict(x,w)
    val expnyt = exp(-1.0 * y * predict)
    val loss = log(1.0 + expnyt)

    val mult = (-y * expnyt)/(1.0 + expnyt)

    // two way
    val x_matrix = xa.t * xa
    var i = 0
    while (i < nrFeat) {
      x_matrix.update(i, i, 0.0)
      i += 1
    }
    // TODO
    //println(w._3.toDenseVector.slice(0,100))
    val grad2 = x_matrix * w._3
    val result2 = (mult :* grad2)

    // one way
    val result1 = (mult :* x.toDenseVector)

    // zero way
    val result0 = mult

    ((result0,result1,result2),predict,loss)
  }

  /*
  * This version _2 is different from the next one, as it doese not use L2 regular part.
  *
  * Input parameters description :
  * w : model for offline
  * valid_x,valid_y : online training data
  * alpha : auxiliary option for learning rate
  * beta : auxiliary option for learning rate
  * lambda1 : regular factor for sparsity , controlling the magnitude of w in form of sum(|w|)
  * lambda2 : regular factor for radius , controlling the magnitude of w in form of sum(w^2). less important option
  * factorLength : factor of factorization machine algorithm
  *
  * General formulation for gradient descent update :
  * w(t + 1) = w(t) - ss(t) * grad(t)
  * Which  w(t) demonstrates current weight point, ss(t) demonstrates current step size (learning rate) ,
  * grad(t) demonstrate current gradient at point w(t)
  *
  * General origin objective formulation for gradient descent update :
  * w(t +1) = argmin_w{ w(t) * x + (1/ss(t)) * (w - w(t))^2 }
  *
  * Intermedian parameters description :
  * eta : auxiliary option for learning rate
  *     eta(t) = alpha / (beta + sqrt(sum_1..t(grad_i^2)))
  * ss : learning rate or step size
  *     sigma(t) = 1/ss(t) = 1/eta(t) - 1/eta(t - 1)
  * ssg : sum of square of gradient
  *     ssg(t) = sum_1..t(grad_i^2)
  * ssn : sum of suggested next point, each of them needed to be compared with sparsity parameter lambda1
  *     ssn(t) = sum_1..t({sigma_i)} * w_i - grad_i)
  *
  * FTRL formulation for descent update global concerned :
  * if ssn(t) > lambda1 and ssn(t) > 0, suggestion will be permitted , w(t + 1) = (ssn(t) - lambda1) / sum_1..t(sigma_i)
  * else ssn(t) < -lambda2 and ssn(t) < 0, suggestion will be permitted , w(t + 1) = (ssn(t) + lambda1) / sum_1..t(sigma_i)
  * esle w(t + 1) = 0.0
  * The simplified presentation is like bellow :
  *  w(t + 1) = (sgn(ssn(t)) * max(0,abs(ssn(t)) - lambda1))/ sum_1..t(sigma_i)
  */
  def train_with_ftrl_l1(w: (Double,BDV[Double],BDM[Double]),valid_x: BDM[Double],valid_y: BDV[Double],
                      alpha: (Double,Double,Double),beta: Double = 1.0,lambda1: (Double,Double,Double),lambda2: (Double ,Double ,Double),
                      factorLength: Int = 20): (Double,BDV[Double],BDM[Double]) = {
    val insNum = valid_x.rows
    val featNum = valid_x.cols

    // initialized by offline model
    var w0 = w._1
    var w1 = w._2
    var w2 = w._3

    // ssn : sum of suggested next point
    var ssn0: Double = 0.0
    var ssn1: BDV[Double] = BDV.zeros[Double](featNum)
    var ssn2: BDM[Double] = BDM.zeros[Double](featNum,factorLength)

    // ssg : sum of square of gradient
    var ssg0: Double = 0.0
    var ssg1: BDV[Double] = BDV.zeros[Double](featNum)
    var ssg2: BDM[Double] = BDM.zeros[Double](featNum,factorLength)

    var i = 0.toInt
    val iter = insNum.toLong
    var hit = 0.toInt
    var lossSum = 0.0.toDouble
    while(i < iter){

      val x: BDV[Double] = valid_x(i,::).t
      val y: Double = valid_y(i)

      val (grad,predict,loss) = computeGrad(x,y,(w0,w1,w2))

      if((signum(predict) * signum(y)) > 0){
        hit += 1
      }
      val acc = (1.0 * hit) / (i + 1)
      lossSum += loss
      val l = lossSum/(i + 1)
      //println(s"iteration ${i} accuracy : ${acc} loss : ${l}")

      // for  bias part
      val sg0 = grad._1 * grad._1
      val eta0 = alpha._1 / (beta + sqrt(ssg0 + sg0))
      val sn0 = (((sqrt(ssg0 + sg0) - sqrt(ssg0)) / alpha._1) * w0) - grad._1
      // update w0
      w0 = signum(ssn0 + sn0) *  max(0.0,abs(ssn0 + sn0) - lambda1._1) * eta0
      // update ssg0 ,ssn0
      ssg0 += sg0
      ssn0 += sn0

      // for one-way part
      val sg1 = grad._2 :* grad._2
      val eta1 = alpha._2 :/ (beta :+ sqrt(ssg1 :+ sg1))
      val sn1 = (((sqrt(ssg1 :+ sg1) :- sqrt(ssg1)) :/ alpha._2) :* w1) :- grad._2
      // update w1
      val zv = BDV.zeros[Double](featNum)
      w1 = (signum(ssn1 :+ sn1) :* max(zv,abs(ssn1 :+ sn1) :- lambda1._2)) :* eta1
      // update ssg1,ssn1
      ssg1 += sg1
      ssn1 += sn1

      // for two-way part
      val sg2 = grad._3 :* grad._3
      val eta2 = alpha._3 :/ (beta :+ sqrt(ssg2 :+ sg2))
      val sn2 = (((sqrt(ssg2 :+ sg2) :- sqrt(ssg2)) :/ alpha._3) :* w2) :- grad._3
      // update w2
      val zm = BDM.zeros[Double](featNum,factorLength)
      w2 = (signum(ssn2 :+ sn2) :* max(zm,abs(ssn2 :+ sn2) :- lambda1._3)) :* eta2
      // update ssg2, ssn2
      ssg2 += sg2
      ssn2 += sn2

      // evaluation
      if((i % 10) == 0){
        val metric = evaluate((w0,w1,w2),valid_x,valid_y,"auc")
        println(s"iteration ${i} metric[auc] ${metric}")
      }

      i += 1
    }

    (w0,w1,w2)
  }

  def train_with_ftrl_l1_l2(w: (Double,BDV[Double],BDM[Double]),valid_x: BDM[Double],valid_y: BDV[Double],
                      alpha: (Double,Double,Double),beta: Double = 1.0,lambda1: (Double,Double,Double),lambda2: (Double ,Double ,Double),
                      factorLength: Int = 20): (Double,BDV[Double],BDM[Double]) = {
    //
    val insNum = valid_x.rows
    val featNum = valid_x.cols

    val rand = breeze.stats.distributions.Gaussian(0, 0.1)
    // weights
    var w0 = 0.0
    var w1 = BDV.zeros[Double](featNum)
    var w2 = BDM.zeros[Double](featNum,factorLength)

    //val metricOffline = evaluate((w0,w1,w2),valid_x,valid_y)

    // initial z for fear that the follwing gradients become zero
    // z(t) = sum(grad(1...t) - sigma(1...t) * w(1...t))
    var z0: Double = rand(3)
    var z1: BDV[Double] = BDV.rand[Double](featNum,rand)
    var z2: BDM[Double] = BDM.rand[Double](featNum,factorLength,rand)

    // q(t) = sum(grad(1...t) * grad(1...t))
    var q0: Double = 0.0
    var q1: BDV[Double] = BDV.zeros[Double](featNum)
    var q2: BDM[Double] = BDM.zeros[Double](featNum,factorLength)

    var hit = 0.toInt
    var i = 0.toInt
    var lossSum = 0.0

    while(i < insNum){

      val x: BDV[Double] = valid_x(i,::).t
      val y: Double = valid_y(i)

      /*
      * compute gradient first , update then
      val (grad,predict) = computeGrad(x,y,(w0,w1,w2))

      val (q,z) = updateWithFTRL(alpha,beta,lambda1,lambda2,(q0,q1,q2),(z0,z1,z2))
      */

      // eta
      val eta0 = alpha._1 / (beta + sqrt(q0))
      val eta1 = alpha._2 :/ (beta :+ sqrt(q1))
      val eta2 = alpha._3 :/ (beta :+ sqrt(q2))

      // update w
      w0 = signum(z0) * max(0.0,abs(z0) - lambda1._1) / (-1.0 * (lambda2._1 + (1.0/eta0)))
      val zv = BDV.zeros[Double](w._2.length)
      // TODO
      //val td1 = z1
      //println(td1.toDenseVector.slice(0,100))
      w1 = (signum(z1) :* max(zv,abs(z1) :- lambda1._2)) :/ (-1.0 :* (lambda2._2 :+ (1.0:/eta1)))
      val zm = BDM.zeros[Double](w._3.rows,w._3.cols)
      // TODO
      //val td0 = abs(z2)
      //println(td0.toDenseVector.slice(0,100))
      w2 = (signum(z2) :* max(zm,(abs(z2) :- lambda1._3))) :/ (-1.0 :* (lambda2._3 :+ (1.0:/eta2)))
      //println(w2.toDenseVector.slice(0,100))

      // compute gradient
      val (grad,predict,loss) = computeGrad(x,y,(w0,w1,w2))

     // TODO
      //println(grad._3.toDenseVector.slice(0,100))

      if((signum(predict) * signum(y)) > 0){
        hit += 1
      }
      val acc = (1.0 * hit) / (i + 1)
      lossSum += loss
      val l = lossSum/(i + 1)
      println(s"iteration ${i} accuracy : ${acc} loss : ${l}")

      // update q,z
      // grad square
      val grad_sqr0 = grad._1 * grad._1
      val grad_sqr1 = grad._2 :* grad._2
      val grad_sqr2 = grad._3 :* grad._3

      val sigma0 = (sqrt(q0 + grad_sqr0) - sqrt(q0)) / alpha._1
      val sigma1 = (sqrt(q1 :+ grad_sqr1) :- sqrt(q1)) :/ alpha._2
      val sigma2 = (sqrt(q2 :+ grad_sqr2) :- sqrt(q2)) :/ alpha._3

      q0 = q0 + grad_sqr0
      q1 = q1 :+ grad_sqr1
      q2 = q2 :+ grad_sqr2

      z0 = z0 + (grad._1 - (sigma0 * w0))
      z1 =  z1 :+ (grad._2 :- (sigma1 :* w1))
      z2 = z2 :+ (grad._3 :- (sigma2 :* w2))

      i += 1
    }

    val metricOnline = 1.0 * hit / insNum
    println(s" METRIC online accuracy : ${metricOnline}")

    (w0,w1,w2)
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

  def readParaDir(dir: String): ArrayBuffer[Double] = {
    val d = new File(dir)
    var files = List[File]()
    if (d.exists && d.isDirectory) {
      files = d.listFiles.filter(_.getName.startsWith("part")).toList
    }

    val paraStrArray = ArrayBuffer[String]()
    var i = 0.toInt
    while(i < files.size) {
      val file = files(i).getAbsoluteFile.toString
      val source = Source.fromFile(file)
      val tmpArray = source.getLines.toArray
      paraStrArray ++= tmpArray
      i += 1
    }
    val paraArray = ArrayBuffer.fill(paraStrArray.length){0.0}
    i = 0.toInt
    while(i < paraStrArray.length){
      val parts = paraStrArray(i).split(":")
      paraArray.update(parts(0).toInt,parts(1).toDouble)
      i += 1
    }

    paraArray
  }

  def loadParams(dir: String,factorLen: Int,featNum: Int): (Double,BDV[Double],BDM[Double]) = {
    val w0dir = dir + "/w0"
    val w1dir = dir + "/w1"
    val w2dir = dir + "/w2"

    val wb0: ArrayBuffer[Double] = readParaDir(w0dir)
    val wb1: ArrayBuffer[Double] = readParaDir(w1dir)
    val wb2: ArrayBuffer[Double] = readParaDir(w2dir)

    val w0 = wb0(0)
    val w1 = BDV(wb1.toArray)
    val w2 = BDM(wb2.toArray).reshape(factorLen,featNum).t

    (w0,w1,w2)
  }

  def writeParam(w: Array[Double],file: String): Unit ={
    val pw = new PrintWriter(new File(file))
    val wstr = Array.fill(w.length){""}
    var i = 0.toInt
    while(i < w.length){
      wstr.update(i,s"${i}:${w(i)}")
      i += 1
    }
    pw.write(wstr.mkString("\n"))
    pw.close
  }

  def save(dir: String,w: (Double,BDV[Double],BDM[Double])): Unit = {

    val w0file = dir + "/w0"
    val w1file = dir + "/w1"
    val w2file = dir + "/w2"

    writeParam(Array(w._1),w0file)
    writeParam(w._2.toArray,w1file)
    writeParam(w._3.toArray,w2file)

  }

  def main(args: Array[String]): Unit = {

    if(args.length < 11){
      println("paras : validatefile parameterdir outputdir alpha0 alpha1 alpha2 lambda0 lambda1 lambda2 factorlen featurenum")
      System.exit(1)
    }

    val validatefile = args(0)
    val paramdir = args(1)
    val outputdir = args(2)
    val alpha0 = args(3).toDouble
    val alpha1 = args(4).toDouble
    val alpha2 = args(5).toDouble
    val beta = 1.0
    val lambda0 = args(6).toDouble
    val lambda1 = args(7).toDouble
    val lambda2 = args(8).toDouble
    val lambdax = 0.4
    val factorLen = args(9).toInt
    val featNum = args(10).toInt

    val (valid_x,valid_y) = loadFile(validatefile)
    println("validate shape : ")
    println(valid_x.rows,valid_x.cols)

    var w = loadParams(paramdir,factorLen,featNum)
    if((w._2.length == 0) || (w._3.size == 0)){
      println("read parameters failed ...")
      System.exit(1)
    }
    println("factor shape : ")
    println(w._3.rows,w._3.cols)

    //w = train_with_ftrl_l1_l2(w,valid_x,valid_y,(alpha0,alpha1,alpha2),beta,(lambda0,lambda1,lambda2),(lambdax,lambdax,lambdax),factorLen)
    w = train_with_ftrl_l1(w,valid_x,valid_y,(alpha0,alpha1,alpha2),beta,(lambda0,lambda1,lambda2),(lambdax,lambdax,lambdax),factorLen)

    save(outputdir,w)

  }
}

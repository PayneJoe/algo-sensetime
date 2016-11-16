
/**
  * Created by yuanpingzhou on 11/16/16.
  *
  * This is implementation of TDAP algorithm based factorization machine, referring http://www.cs.cmu.edu/~epxing/papers/2016/HuaWei_KDD16.pdf
  * authored by HUAWEI&CMU
  *
  * TDAP is mainly modified on the step size of FTRL, and it not only depends the gradient of current point ,but also relys on current time
  *
  */
package com.sensetime.ad.algo.ctr

object FMWithTDAP {

  import java.io._

  import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV, norm => brzNorm, _}
  import breeze.numerics._

  import scala.collection.mutable.ArrayBuffer
  import scala.io.Source

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

  def evaluate(w: (Double,BDV[Double],BDM[Double]),data_x: BDM[Double],data_y: BDV[Double]): Double = {

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

    val ret = computeAccuracy(predict,data_y)
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
  * siss : sum of inverse of step size
  *     siss(t) = sum_1..t(sigma_i)
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
  def train_with_tdap_l1(w: (Double,BDV[Double],BDM[Double]),valid_x: BDM[Double],valid_y: BDV[Double],
                      alpha: (Double,Double,Double),beta: Double = 1.0,lambda1: (Double,Double,Double),lambda2: (Double ,Double ,Double),
                      factorLength: Int = 20): (Double,BDV[Double],BDM[Double]) = {
    val insNum = valid_x.rows
    val featNum = valid_x.cols

    // TODO
    val tau = 1.0

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

    // sum of inverse of step size
    var siss0: Double = 0.0
    var siss1: BDV[Double] = BDV.zeros[Double](featNum)
    var siss2: BDM[Double] = BDM.zeros[Double](featNum,factorLength)

    // sum of time decaying factor
    var stdf = 0.0

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
      println(s"iteration ${i} accuracy : ${acc} loss : ${l}")

      // for time decaying factor f(t)
      val t = i + 1
      val tdf = exp(-1.0 * ((t - 1 + 1) / (2.0 * tau * tau)))

      // for  bias part
      val sg0 = grad._1 * grad._1
      // inverse of step size in iteration i
      val sigma0 = (sqrt(ssg0 + sg0) - sqrt(ssg0)) / alpha._1
      // inserse of step size * time decaying factor
      val iss0 = sigma0 * tdf
      // suggested next point
      val sn0 = (iss0 * w0) - grad._1
      // update w0
      w0 = signum(ssn0 + sn0) *  max(0.0,abs(ssn0 + sn0) - lambda1._1) / (siss0 + iss0)
      // update ssg0 ,ssn0 , siss0
      ssg0 += sg0
      ssn0 += sn0
      siss0 += iss0

      // for one-way part
      val sg1 = grad._2 :* grad._2
      // inverse of step size in iteration i
      val sigma1 = (sqrt(ssg1 :+ sg1) :- sqrt(ssg1)) :/ alpha._2
      // inverse of step size * time decaying factor
      val iss1 = sigma1 :* tdf
      // suggested next point
      val sn1 = (iss1 :* w1) :- grad._2
      // update w1
      val zv = BDV.zeros[Double](featNum)
      w1 = (signum(ssn1 :+ sn1) :* max(zv,abs(ssn1 :+ sn1) :- lambda1._2)) :/ (siss1 :+ iss1)
      // update ssg1,ssn1, siss1
      ssg1 = ssg1 :+ sg1
      ssn1 = ssn1 :+ sn1
      siss1 = siss1 :+ iss1

      // for two-way part
      val sg2 = grad._3 :* grad._3
      // invserse of step size in iteration i
      val sigma2 = (sqrt(ssg2 :+ sg2) :- sqrt(ssg2)) :/ alpha._3
      // inverse of step size * time decaying factor
      val iss2 = sigma2 :* tdf
      // suggested next point
      val sn2 = (iss2 :* w2) :- grad._3
      // update w2
      val zm = BDM.zeros[Double](featNum,factorLength)
      w2 = (signum(ssn2 :+ sn2) :* max(zm,abs(ssn2 :+ sn2) :- lambda1._3)) :/ (siss2 :+ iss2)
      // update ssg2, ssn2 , siss2
      ssg2 = ssg2 :+ sg2
      ssn2 = ssn2 :+ sn2
      siss2 = siss2 :+ iss2

      //TODO print learning rate per ieteration
      println(iss2.toDenseVector.slice(0,100))

      i += 1
    }

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

    w = train_with_tdap_l1(w,valid_x,valid_y,(alpha0,alpha1,alpha2),beta,(lambda0,lambda1,lambda2),(lambdax,lambdax,lambdax),factorLen)

    save(outputdir,w)

  }
}

/**
  * Created by yuanpingzhou on 11/8/16.
  */
package com.sensetime.ad.algo.ctr

object FMWithFTRL {

  import scala.io.Source
  import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV, norm => brzNorm, _}
  import scala.collection.mutable.ArrayBuffer
  import breeze.numerics.{exp,sqrt,log,abs}
  import breeze.numerics.signum

  import java.io._

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

  // lambda1 : regular option for l1 of {w0,w1,w2} , the exact regular regular expression in update function
  // lambda2 : regular option for l2 of {w0,w1,w2} , not the exact regular expression in update function
  // grad : gradient option for {w0,w1,w2}
  // alpha : learning rate option for {w0,w1,w2} , share the same one
  // beta : fix value 1.0 for {w0,w1,w2} ,share the same one
  // q : accumulated (grad * grad) for each element of {w0,w1,w2} , e.g. q2(i)(j)
  // z : accumulated (grad - sigma * w) for each element of {w0,w1,w2} , e.g. z2(i)(j)
  /*def updateWithFTRL(alpha: Double,beta: Double,
                     lambda1: (Double,Double,Double),lambda2: (Double,Double,Double),q: (Double,BDV[Double],BDM[Double]),
                     z: (Double,BDV[Double],BDM[Double])):
                    ((Double,BDV[Double],BDM[Double]),(Double,BDV[Double],BDM[Double]),(Double,BDV[Double],BDM[Double])) =

  {
    // grad square
    val grad_sqr0 = grad._1 * grad._1
    val grad_sqr1 = grad._2 :* grad._2
    val grad_sqr2 = grad._3 :* grad._3

    // sigma
    val sigma0 = (sqrt(q._1 + grad_sqr0) - sqrt(q._1)) / alpha
    val sigma1 = (sqrt(q._2 :+ grad_sqr1) :- sqrt(q._2)) :/ alpha
    val sigma2 = (sqrt(q._3 :+ grad_sqr2) :- sqrt(q._3)) :/ alpha

    // q
    val q0 = q._1 + grad_sqr0
    val q1 = q._2 :+ grad_sqr1
    val q2 = q._3 :+ grad_sqr2

    // z
    val z0 = z._1 + grad._1 - (sigma0 * w._1)
    val z1 = z._2 :+ (grad._2 :- (sigma1 :* w._2))
    val z2 = z._3 :+ (grad._3 :- (sigma2 :* w._3))

    // eta
    val eta0 = alpha / (beta + sqrt(q0))
    val eta1 = alpha :/ (beta :+ sqrt(q1))
    val eta2 = alpha :/ (beta :+ sqrt(q2))

    // update w
    val w0 = signum(z0) * max(0.0,abs(z0) - lambda1._1) / (-1.0 * (lambda2._1 + (1.0/eta0)))
    val zv = BDV.zeros[Double](w._2.length)
    val w1 = (signum(z1) :* max(zv,abs(z1) :- lambda1._2)) :/ (-1.0 :* (lambda2._2 :+ (1.0:/eta1)))
    val zm = BDM.zeros[Double](w._3.rows,w._3.cols)
    val w2 = (signum(z2) :* max(zm,abs(z2) :- lambda1._3)) :/ (-1.0 :* (lambda2._3 :+ (1.0:/eta2)))

    ((w0,w1,w2),(q0,q1,q2),(z0,z1,z2))
  }
  */

  def train_with_ftrl(w: (Double,BDV[Double],BDM[Double]),valid_x: BDM[Double],valid_y: BDV[Double],
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
      val td1 = z1
      //println(td1.toDenseVector.slice(0,100))
      w1 = (signum(z1) :* max(zv,abs(z1) :- lambda1._2)) :/ (-1.0 :* (lambda2._2 :+ (1.0:/eta1)))
      val zm = BDM.zeros[Double](w._3.rows,w._3.cols)
      // TODO
      val td0 = abs(z2)
      println(td0.toDenseVector.slice(0,100))
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

    w = train_with_ftrl(w,valid_x,valid_y,(alpha0,alpha1,alpha2),beta,(lambda0,lambda1,lambda2),(lambdax,lambdax,lambdax),factorLen)

    save(outputdir,w)

  }
}

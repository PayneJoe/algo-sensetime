import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg.{min, DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV, norm => brzNorm}
import org.apache.spark.rdd.RDD
import breeze.numerics.{exp, log, signum, sqrt}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.HashPartitioner
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by yuanpingzhou on 10/25/16.
  */
object test {

  def seqFunc(a: Int,b: Int): Int = {
    println(s"${a} : ${b}")
    min(a,b)
  }

  def comFunc(a: Int,b: Int): Int = {
    println(s"${a} + ${b}")
    a+b
  }

  def func(v: BDV[Double]) = {
    val x = BDV.fill[Double](5,100.0)
    v.values
  }

  def main(args: Array[String]): Unit = {
    /*val conf = new SparkConf().setAppName("test").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val data = sc.parallelize(2 to 200,50)
    data.treeAggregate(100)(seqFunc,comFunc,3)

    val v = BDV.zeros[Double](5)
    func(v)
    */
    val a = BDV(Array(0.92,-0.24,0.43,0.69,-0.40))
    val b = BDV(Array(1,-1,1,-1,-1))
    val _a = signum(a)
    val _b = signum(b)
    val c = ((_a :* _b).toArray.filter(_>0).sum * 1.0)/_a.length
    println(c)
  }
}

import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV, norm => brzNorm, _}
import org.apache.spark.rdd.RDD
import breeze.numerics.{exp, log, sqrt}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.HashPartitioner

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

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val data = sc.parallelize(2 to 200,50)
    data.treeAggregate(100)(seqFunc,comFunc,3)

  }
}

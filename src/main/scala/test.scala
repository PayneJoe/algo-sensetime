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

  def main(args: Array[String]): Unit = {
    /*
    val conf = new SparkConf().setAppName("test").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val d0 = Array((0,0.6),(1,0.8),(4,0.2),(3,0.5),(5,0.9))
    val d1 = Array((0,10),(0,60),(1,70),(0,20),(1,50),(0,100))
    val tmp0 = sc.parallelize(d0)
    val tmp1 = sc.parallelize(d1)
    val model = tmp0.partitionBy(new HashPartitioner(2))
    val data = tmp1.partitionBy(new HashPartitioner(2))
    val ret = model.mapPartitions{
      p =>
        var cnt = 0.toInt
        p.foreach{
          r =>
            cnt += 1
        }
        Iterator.single(cnt)
    }
    println(ret.collect().toVector)
    */

    /*
    val data = ArrayBuffer[(String,Int)]()
    data.append(("joe",27))
    data.append(("sawyer",25))
    data.append(("ben",37))
    data.append(("kate",28))
    data.append(("joe",52))
    data.append(("ben",29))
    data.append(("smith",32))
    data.append(("sawyer",40))
    data.append(("kate",35))
    var tmpRdd = sc.parallelize(data)
    val dataRdd = tmpRdd.keyBy(_._1).partitionBy(new HashPartitioner(3)).values
    dataRdd.foreachPartition{
      r =>
        println(r.toVector)
    }

    val coefficients = ArrayBuffer[(String,Double)]()
    coefficients.append(("joe",0.01))
    coefficients.append(("ben",0.1))
    coefficients.append(("sawyer",0.2))
    coefficients.append(("smith",0.5))
    val tmpRdd2 = sc.parallelize(coefficients)
    val modelRdd = tmpRdd2.keyBy(_._1).partitionBy(new HashPartitioner(2)).values
    modelRdd.foreachPartition{
      r =>
        println(r.toVector)
    }
    */
    val rand = breeze.stats.distributions.Gaussian(0, 0.1)
    val arr = Array(1,2,3,4,5,6,7,8)
    var w = BDM(arr)
    w = w.reshape(4,2).t.toDenseMatrix
    println(w)
    val w1 = w(::,0 to 0)
    val w2 = w(::,2 to (w.cols - 1))

    val w3 = BDM.horzcat(w1,w2)
    println(w3)

    val a1 = BDV(1.0, 2.0, 3.0)
    val a2 = BDV(0.6, 0.2, 0.8)
    val a3 = BDV.vertcat(a1, a2)
    println(a3)

    val pair = a1.toArray.zip(a2.toArray)
    val sortedPair = pair.sortBy(_._2)
    println(sortedPair.toVector)
    println(sortedPair.slice(0,3).toVector)


  }
}

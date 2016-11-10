import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV, norm => brzNorm, _}
import org.apache.spark.rdd.RDD
import breeze.numerics.sqrt
import scala.collection.mutable.ArrayBuffer

/**
  * Created by yuanpingzhou on 10/25/16.
  */
object test {
  def getsum(a: Array[Double],b: BV[Double]): BDM[Double] = {
    //val p1 = BDV(a.slice(0,2))
    //val p2 = BDV(a.slice(2,4))
    //val mul = BDM(p1.t,p2.t)
    val mul: BDM[Double] = BDM((a(0),a(2)),(a(1),a(3)))
    val ret = b.toDenseVector.asDenseMatrix * mul
    println(a.toVector,ret)
    ret
  }

  def main(args: Array[String]): Unit = {
    //val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    ///val sc = new SparkContext(conf)
    /*
    val share = BV(100.toDouble, 200.toDouble)
    val bcshare = sc.broadcast(share)
    val data = sc.parallelize(100 to 120,4)
    val dataRdd = sc.makeRDD(Array(Array(0.2, 0.4, 0.1, 0.1), Array(0.8, 0.6, 0.1, 0.1), Array(0.1, 0.2, 0.1, 0.1),
      Array(0.2, 0.3, 0.1, 0.1), Array(0.2, 0.5, 0.1, 0.1), Array(0.7, 0.4, 0.1, 0.1)), 8)
    val dataRdd1 = dataRdd.map(
      record => {
        getsum(record, bcshare.value)
      }
    )
    dataRdd1.map(w => w.map(_/5)).reduce(_+_).data.foreach(println)
    val ta = Array(0.8,0.4,0.3)
    val tb = Array(1,1,0)
    val tc = BDV(ta.map(
      v => {
        if (v > 0.5)
          1.toDouble
        else
          0.toDouble
      }
    ):* tb.map(_.toDouble))
   val hit = BDV(ta.zip(tb).map(v =>{
      if(((v._1.toDouble > 0.5)&&(v._2 == 1)) || ((v._1.toDouble <= 0.5) && (v._2 == 0))){
        1
      }
      else
        0
   } ))
   println(hit.sum)
   val m: BDM[Double] = BDM.rand(2,3)
   m.map(v => println(v))
   */
    /*
    val train = Array(10,20,30,40,50)
    val trainRDD = sc.makeRDD(train,2)
    val (total,count) = trainRDD.mapPartitions{
      partition =>
        var localTotal = 0.0
        var count = 0
        partition.foreach{
          record =>
            localTotal += 0.1 * record
            count += 1
        }
        Iterator.single((localTotal,count))
    }.treeReduce{
      case((w1,c1),(w2,c2)) => {
        //println(s"${w1} ${c1} ${w2} ${c2}")
        //(((w1 * c1) + (w2 * c2))/(c1 + c2),(c1 + c2))
        (w1 + w2,c1 + c2)
      }
    }
    */

    /*
    val d = BDV(v0.toArray) :- BDV(v1.toArray)
    val diff = sqrt((d :* d).sum/v0.length)
    println(diff)
    val c0 = v0.filter(_>0.0).length
    val c1 = v1.filter(_>0.0).length
    println(c0,c1)
    */

    //val a = BDV[Double](10.0,20.0,30.0)
    //val k = BDV(100.0,200.0,300.0) + 0.1.toDouble :* a
    //println(k)
    //val a: ArrayBuffer[Double] = new ArrayBuffer[Double]()
    /*
    val b: ArrayBuffer[Double] = ArrayBuffer(1.0,2.0,3.0,8.0,10.0,80.0,3.5,6.7)
    val c: ArrayBuffer[Double] = ArrayBuffer(10.0,20.0,30.0,4.0,100.7,6.2,7.8,9.5)
    val d: ArrayBuffer[ArrayBuffer[Double]] = new ArrayBuffer[ArrayBuffer[Double]]()

    d.append(b)
    d.append(c)
    val a: BDM[Double] = BDM(d:_*)
    val k = a.t.toArray

    val kb = new ArrayBuffer[String]()
    var i = 0.toInt
    while(i < k.length){
      kb += s"${i}:${k(i)}"
      i += 1
    }
    println(kb)

    val drdd = sc.parallelize(kb)
    val outputDir = "/Users/yuanpingzhou/data/spark/sparktest/FMWithSGD/output/test/"
    drdd.repartition(1)
    drdd.saveAsTextFile(outputDir + "d")

    val rd = sc.textFile(outputDir + "d")
    val rb = rd.collect
    val db = Array.fill(rb.length){0.0}
    rb.foreach{
      v =>
        val p = v.split(":")
        db.update(p(0).toInt,p(1).toDouble)
    }
    val newd = BDM(db).reshape(8,2).t
    println(newd.rows,newd.cols)
    */
    val t = BDM(10.0,-20.0,30.0,-40.0,50.0,60.0,5.7,6.8)
    val tt = t.reshape(4,2).t
    val z = BDM.zeros[Double](2,4)
    val ret = max(z,tt)
    println(ret)

    val l = List(0,2)
    val sm = ret(::,l)
    println(sm)
    val sm0 = sm.toDenseMatrix

    println(sum(ret(::,l)))
    println(sum(sm0(::,*)))
    println(sum(sm))

  }
}

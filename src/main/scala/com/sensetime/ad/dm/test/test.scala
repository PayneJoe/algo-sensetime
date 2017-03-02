package com.sensetime.ad.dm.test

import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg.{min, DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV, norm => brzNorm}
import breeze.numerics.{exp, log, signum, sqrt}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by yuanpingzhou on 10/25/16.
  */
object test {

  def transposeRowMatrix(m: RowMatrix): RowMatrix = {
    val transposedRowsRDD = m.rows.zipWithIndex.map{case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex)}
      .flatMap(x => x) // now we have triplets (newRowIndex, (newColIndex, value))
      .groupByKey
      .sortByKey().map(_._2) // sort rows and remove row indexes
      .map(buildRow) // restore order of elements in each row and remove column indexes
    new RowMatrix(transposedRowsRDD)
  }

  def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
    val indexedRow = row.toArray.zipWithIndex
    indexedRow.map{case (value, colIndex) => (colIndex.toLong, (rowIndex, value))}
  }

  def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
    val resArr = new Array[Double](rowWithIndexes.size)
    rowWithIndexes.foreach{case (index, value) =>
      resArr(index.toInt) = value
    }
    Vectors.dense(resArr)
  }

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
//    val conf = new SparkConf().setMaster(args(0)).setAppName("CosineSimilarity")
//    val sc = new SparkContext(conf)
//    val sqlContext = new HiveContext(sc)
//
//    // Load and parse the data file.
//    val rows = sc.parallelize(0 to 10).map{
//      case idx =>
//        val featVec = BDV.rand[Double](5)
//        (s"user${idx}",Vectors.dense(featVec.toArray))
//    }
//    val users = rows.map(_._1)
//    val features = rows.map(_._2)
//    //convert that RDD to an RDD[IndexedRow]
//    val indexedRDD = features.zipWithIndex.map{
//      case(value, index) => IndexedRow(index, value)
//    }
//    //make a matrix
//    val matrix = new IndexedRowMatrix(indexedRDD)
//    //calculate the distributions
//    val exact = matrix.toCoordinateMatrix.transpose().toIndexedRowMatrix().columnSimilarities()
//    val exactEntries = exact.entries.map { case MatrixEntry(i, j, u) => ((i, j), u) }
//    println(exactEntries.collect().length)
//    // Compute similar columns with estimation using DIMSUM
//    val approx = mat.columnSimilarities(0.1)
//

//    val approxEntries = approx.entries.map { case MatrixEntry(i, j, v) => ((i, j), v) }
//    val MAE = exactEntries.leftOuterJoin(approxEntries).values.map {
//      case (u, Some(v)) =>
//        math.abs(u - v)
//      case (u, None) =>
//        math.abs(u)
//    }.mean()
//    print(s"------ ${MAE}")

  }
}

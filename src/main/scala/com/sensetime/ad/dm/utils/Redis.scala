package com.sensetime.ad.dm.utils


/**
  * Created by yuanpingzhou on 1/13/17.
  */
import com.sensetime.ad.dm.utils.ExceptionPool.RankingException

import redis.clients.jedis.{Jedis, Response}
import scala.collection.JavaConverters._

import breeze.linalg.{DenseVector => BDV}

class Redis(private val host: String,private val port: Int) {

  private val handle = new Jedis(host,port)

  // get newest key from a sorted set
  def getKey(dbNum: Int): String = {
    val jd = this.handle
    var modelKey = ""
    try {
      jd.select(dbNum)
      modelKey = jd.zrange("model_index", -1, -1).iterator().next()
    }
    catch{
      case e: Exception =>
        //  DEBUG
        // println(e.getMessage)
    }
    finally {
      // TODO
    }
    // assert result
    if((modelKey == null) || modelKey.isEmpty){
      throw new RankingException("get key failed")
    }
    else {
      modelKey
    }
  }

  /*
   * get hash record with specific db and key
   * @dbNum : database number
   * @keyStr : hash key
   *
   */
  def getHashRecord(dbNum: Int,keyStr: String): Map[String,String] = {
    val jd = this.handle
    var tryTimes = 3
    var flag = false
    var tmpResult: Response[java.util.Map[String,String]] = null
    // try more times
    while(tryTimes > 0 && !flag){
      try{
        val pp = jd.pipelined()
        pp.select(dbNum)
        tmpResult = pp.hgetAll(keyStr)
        pp.sync()
        flag = true
      }
      catch{
        case e: Exception =>
          flag = false
          tryTimes -= 1
      }
      finally {
        // TODO
      }
    }
    // assert result
    if((tmpResult == null) || (tmpResult.get().size() == 0)){
      throw new RankingException("get row failed")
    }
    else {
      tmpResult.get().asScala.toMap
    }
  }

  /*
   * insert multiple hash records
   * @dbNum : database number
   * @records : Map(key -> Map(field -> value))
   *
   */
  def insertMultHashRecord[T](dbNum: Int,records: Map[String,Map[String,T]]) = {
    val jd = this.handle
    var tryTimes = 4
    var flag = false
    // try more times
    while(tryTimes > 0 && !flag){
      try{
        val pp = jd.pipelined()
        pp.select(dbNum)
        records.foreach{
          case (aUser,neighbors) =>{
            val fields = neighbors.mapValues(_.toString).toList
            pp.hmset(aUser,fields.toMap.asJava)
          }
        }
        pp.sync()
        flag = true
      }
      catch{
        case e: Exception =>
          flag = false
          if(tryTimes == 1){
            throw new RankingException("insert 3 times , unfortunately all failed .")
          }
          tryTimes -= 1
      }
      finally {
        // TODO
      }
    }
  }

  /*
   * Insert hash record
   * @dbNum: database number
   * @key: row key
   * @record: Map(field -> value)
   *
   */
  def insertHashRecord[T](dbNum: Int,key: String,record: Map[String,T]) = {
    val jd = this.handle
    var tryTimes = 4
    var flag = false
    // try more times
    while(tryTimes > 0 && !flag){
      try{
        val pp = jd.pipelined()
        pp.select(dbNum)
        pp.hmset(key,record.mapValues(_.toString).toList.toMap.asJava)
        pp.sync()
        flag = true
      }
      catch{
        case e: Exception =>
          flag = false
          if(tryTimes == 1){
            throw new RankingException("insert 3 times , unfortunately all failed .")
          }
          tryTimes -= 1
      }
      finally {
        // TODO
      }
    }
  }

  /*
   * insert multiple hash fields
   * @dbNum : database number
   * @records : List((key,field) -> value)
   *
   */
  def insertMultHashField[T](dbNum: Int,fields: List[((String,String),T)]) = {
    val jd = this.handle
    var tryTimes = 4
    var flag = false
    // try more times
    while(tryTimes > 0 && !flag){
      try{
        val pp = jd.pipelined()
        pp.select(dbNum)
        fields.foreach{
          case (k,v) => {
            pp.hset(k._1,k._2,v.toString)
          }
        }
        pp.sync()
        flag = true
      }
      catch{
        case e: Exception =>
          flag = false
          if(tryTimes == 1){
            throw new RankingException("insert 3 times , unfortunately all failed .")
          }
          tryTimes -= 1
      }
      finally {
        // TODO
      }
    }
  }

  // TODO, need to be generalized
  def insertSortedRecord(dbNum: Int,sortedKey: Long,recordKey: String,model: BDV[Double],features: List[String]) = {
    val jd = this.handle
    var tryTimes = 4
    var flag = false
    val indexedModel = features.zip(model.toArray.map(_.toString))
    // try more times
    while(tryTimes > 0 && !flag){
      try{
        val pp = jd.pipelined()
        pp.select(dbNum)
        pp.zadd("model_index",sortedKey,recordKey)
        pp.hmset(recordKey,indexedModel.toMap.asJava)
        pp.sync()
        flag = true
      }
      catch{
        case e: Exception =>
          flag = false
          if(tryTimes == 1){
            throw new RankingException("insert 3 times , unfortunately all failed .")
          }
          tryTimes -= 1
      }
      finally {
        // TODO
      }
    }
  }

  def disconnect() ={
    this.handle.disconnect()
  }
}

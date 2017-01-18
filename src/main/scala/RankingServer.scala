package com.sensetime.ad.algo.ranking

/**
  * Ranking server for user Recommendation System , send ranked list after receiving list request
  *
  * Created by yuanpingzhou on 1/13/17.
  */

import scalaz.concurrent.Task
import org.http4s.server.{Server, ServerApp}
import org.http4s.server.blaze._
import org.http4s._
import org.http4s.dsl._
import breeze.numerics.exp

import scala.util.{Try,Success,Failure}

import com.sensetime.ad.algo.utils._
import com.sensetime.ad.algo.ranking.RankingParameters._
import com.sensetime.ad.algo.utils.ExceptionPool._
/*
import org.http4s.circe._
import io.circe.generic.auto._
import io.circe.syntax._
*/
object RankingServer extends ServerApp {

  private var fixedModel: Map[String,Double] = _

  private val host = "localhost"
  private val port = 6379
  private val fixedModelDBNum = 0
  private val randomUserDBNum = 1
  private val randomFeatureDBNum = 2
  private val userInfoDBNum = 3
  private val featureInfoDBNum = 4

  private def loadData (dbNum: Int,keyStr : String = ""): (Map[String,String]) = {

    val dbHandle = new Redis(this.host,this.port)

    var result: Map[String,String] = null
    dbNum match {
      case 0 => { // fixed effect model , whose key is model identifier
        try {
          val modelId = dbHandle.getKey(dbNum)
          val rowKey = modelId
          result = dbHandle.getRecord(dbNum, rowKey)
        }
        catch{
          case e: Exception =>
            dbHandle.disconnect()
            throw e
        }
        finally {
          // success
          dbHandle.disconnect()
        }
      }
      case 1 => { // random effect model for user , whose key is combination of model identifier and random effect id
        // TODO
        dbHandle.disconnect()
        throw new RankingException(s"Load data exception , db ${dbNum} key ${keyStr}")
      }
      case 2 if (keyStr.isEmpty == false) => { // random effect model for feature , whose key is combination of model identifier and random effect id
        try {
          val modelId = dbHandle.getKey(dbNum)
          val rowKey = s"${modelId}_${keyStr}"
          result = dbHandle.getRecord(dbNum, rowKey)
        }
        catch {
          case e: Exception =>
            dbHandle.disconnect()
            throw e
        }
        finally {
          // success
          dbHandle.disconnect()
        }
      }
      case 3 if (keyStr.isEmpty == false) => { // user info table , use_id is the key
        try {
          val rowKey = keyStr
          result = dbHandle.getRecord(dbNum, rowKey)
        }
        catch{
          case e: Exception =>
            dbHandle.disconnect()
            throw e
        }
        finally {
          // success
          dbHandle.disconnect()
        }
      }
      case 4 => { // feature info table
        // TODO
        dbHandle.disconnect()
        throw new RankingException(s"Load data exception , db ${dbNum} key ${keyStr}")
      }
      case _ => {
        dbHandle.disconnect()
        throw new RankingException(s"Load data exception , db ${dbNum} key ${keyStr}")
      }
    }

    result
  }
  // http server test with POST method , whose body is formatted with json
  val jsonService = HttpService {
    /*
    // Handled with circe library , working not well with scala2.10.*
    // referring http://http4s.org/v0.15/json/
    case r @ POST -> Root / "list_request" =>
      r.as(jsonOf[ListRequest]).flatMap{
        listRequest =>
          val featList = listRequest.featList.split(",",-1)
          val ranked = featList.sortWith((s0: String,s1: String) => s0.compare(s1) > 0).mkString(",")
          Ok(ListResponse(listRequest.userId,listRequest.listSessionId,System.currentTimeMillis()/1000,ranked).asJson)
      }
    */

    // Handled with argonaut library , perfectly working both with scala 2.10.* and 2.11.*
    // referring https://github.com/underscoreio/http4s-example/blob/master/src/main/scala/underscore/example/Example.scala
    case req @ POST -> Root / "list_request" => {
      req.decode[ListRequest] {
        case data => {
          val userId = data.userId
          val listSessionId = data.listSessionId
          val timestamp = System.currentTimeMillis() / 1000
          val featList = data.featList.split(",", -1)
          fixedModel match {
            case null => {
              println(s"Loading model failed , ranking server will not work.")
              Ok(ListResponse(data.userId, data.listSessionId, data.timestamp, data.featList))
            }
            case _ => {
              Try(loadData(userInfoDBNum, userId)) match {
                case Success(ui) => {
                  val userInfo = ui
                  // fixed effect feature part
                  val fixedEffectModelActivated = userInfo.map {
                    case (k, v) => {
                      val featKey = s"${k}_${v}"
                      fixedModel.get(featKey).getOrElse(.0)
                    }
                  }
                  // random effect feature part
                  var probabilityMap = Map[String, Double]()
                  featList.foreach {
                    case featId =>
                      var score = fixedEffectModelActivated.sum
                      Try(loadData(randomFeatureDBNum, featId).mapValues(_.toDouble)) match {
                        case Success(rem) =>
                          val randomEffectModel = rem
                          val randomEffectModelActivated = userInfo.map {
                            case (k, v) =>
                              val featKey = s"${k}_${v}"
                              randomEffectModel.get(featKey).getOrElse(.0)
                          }
                          score += randomEffectModelActivated.sum
                        case Failure(msg) =>
                          println(s"Loading random effect model specified by ${featId} failed, error : ${msg.getMessage}")
                      }
                      //println(featId,score)
                      val probability = 1.0 / (1.0 + exp(-1.0 * score))
                      probabilityMap ++= Map(featId -> probability)
                  }
                  val rankedFeatList = probabilityMap.toSeq.sortWith(_._1 > _._1).map(v => v._1).mkString(",")
                  Ok(ListResponse(userId, listSessionId, timestamp, rankedFeatList))
                }
                case Failure(msg) => {
                  println(s"Loading user info specified by ${userId} failed , ranking server will not work . Error : ${msg.getMessage}")
                  Ok(ListResponse(data.userId, data.listSessionId, data.timestamp, data.featList))
                }
              }
            }
          }
        }
      }
    }
    // TODO : proper exception handling
    case _ =>
      Ok("ok")
  }

  override def server(args: List[String]): Task[Server] = {

    Try(loadData(fixedModelDBNum).mapValues(_.toDouble)) match {
      case Success(v) => {
        fixedModel = v
      }
      case Failure(msg) =>{
        fixedModel = null
      }
    }

    BlazeBuilder
      .bindHttp(1024, "localhost")
      .mountService(jsonService, "/")
      .start
  }
}

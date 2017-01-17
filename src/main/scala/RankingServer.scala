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

import com.sensetime.ad.algo.utils._
import com.sensetime.ad.algo.ranking.RankingParameters._
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
      val modelId = dbHandle.getKey(dbNum)
        val rowKey = modelId
        result = dbHandle.getRecord(dbNum,rowKey)
      }
      case 1 => { // random effect model for user , whose key is combination of model identifier and random effect id
        // TODO
      }
      case 2 => { // random effect model for feature , whose key is combination of model identifier and random effect id
      val modelId = dbHandle.getKey(dbNum)
        val rowKey = s"${modelId}_${keyStr}"
        result = dbHandle.getRecord(dbNum,rowKey)
      }
      case 3 => { // user info table , use_id is the key
      val rowKey = keyStr
        result = dbHandle.getRecord(dbNum,rowKey)
      }
      case 4 => { // feature info table
        // TODO
      }
    }

    result
  }

  // http server test with GET method
  val services = HttpService {
    case GET -> Root / "user=" / user_id => Ok(s"Hello , ${user_id}")
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
        case data =>
          val userId = data.userId
          val listSessionId = data.listSessionId
          val featList = data.featList.split(",", -1)
          val userInfo = loadData(userInfoDBNum,userId)
          // fixed effect feature part
          val fixedEffectModelActivated = userInfo.map{
            case (k,v) => {
              val featKey = s"${k}_${v}"
              fixedModel.get(featKey).getOrElse(.0)
            }
          }
          // random effect feature part
          var probabilityMap = Map[String,Double]()
          featList.foreach{
            case featId =>
              val randomEffectModel = loadData(randomFeatureDBNum,featId).mapValues(_.toDouble)
              val randomEffectModelActivated = userInfo.map{
                case (k,v) =>
                  val featKey = s"${k}_${v}"
                  randomEffectModel.get(featKey).getOrElse(.0)
              }

              val score = fixedEffectModelActivated.sum + randomEffectModelActivated.sum
              println(featId,score)
              val probability = 1.0/(1.0 + exp(-1.0 * score))
              probabilityMap ++= Map(featId -> probability)
          }
          val rankedFeatList = probabilityMap.toSeq.sortWith(_._1 > _._1).map(v => v._1).mkString(",")
          //val ranked = featList.sortWith((s0: String, s1: String) => s0.compare(s1) > 0).mkString(",")
          Ok(ListResponse(data.userId, data.listSessionId, data.timestamp, rankedFeatList))
      }
    }
    // TODO : proper exception handling
    case _ =>
      Ok("ok")
  }

  override def server(args: List[String]): Task[Server] = {
    fixedModel = loadData(fixedModelDBNum).mapValues(_.toDouble)

    BlazeBuilder
      .bindHttp(1024, "localhost")
      .mountService(jsonService, "/")
      .start
  }
}

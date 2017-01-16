package com.sensetime.ad.algo.ranking

/**
  * Created by yuanpingzhou on 1/15/17.
  */
object RankingParameters {

  import _root_.argonaut._
  import org.http4s._
  import org.http4s.argonaut._
  import org.http4s.server._
  import org.http4s.dsl._


  case class User(name: String)
  // defined class User

  case class Hello(greeting: String)
  // defined class Helo

  final case class ListRequest(userId: String,listSessionId: String,timestamp: Long,featList: String)
  object ListRequest {
    implicit val listRequestCodecJson: CodecJson[ListRequest] =
      Argonaut.casecodec4(ListRequest.apply, ListRequest.unapply)("userId", "listSessionId", "timestamp", "featList")
    implicit val listRequestEntityDecoder: EntityDecoder[ListRequest] =
      jsonOf[ListRequest]
    implicit val listRequestEntityEncoder: EntityEncoder[ListRequest] =
      jsonEncoderOf[ListRequest]
  }

  final case class ListResponse(userId: String,listSessionId: String,timestamp: Long,featList: String)
  object ListResponse{
    implicit val listResponseCodecJson: CodecJson[ListResponse] =
      Argonaut.casecodec4(ListResponse.apply, ListResponse.unapply)("userId", "listSessionId", "timestamp", "featList")
    implicit val listResponseEntityDecoder: EntityDecoder[ListResponse] =
      jsonOf[ListResponse]
    implicit val listResponseEntityEncoder: EntityEncoder[ListResponse] =
      jsonEncoderOf[ListResponse]
  }
}

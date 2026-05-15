package com.footlocker.utils
import play.api.libs.json._

case class UserAgentData(
                          browser: Map[String, String],
                          os: Map[String, String],
                          device: Map[String, String],
                          stringVersion: String
                        )

object UserAgentData {
  implicit val format: OFormat[UserAgentData] = Json.format[UserAgentData]
}

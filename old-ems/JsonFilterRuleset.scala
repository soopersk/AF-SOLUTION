package com.example.filter // Adjust package as needed

import com.fasterxml.jackson.annotation.JsonAnySetter
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.PathNotFoundException
import scala.util.Try
import scala.util.matching.Regex

class JsonFilterRuleset {

  @JsonAnySetter
  var rules: Map[String, String] = Map.empty

  def filter(eventJson: String): Boolean = {
    if (rules.isEmpty) return false

    val lowerJson = eventJson.toLowerCase

    rules.forall { case (path, value) =>
      val filterValue = value.toLowerCase
      
      Try(JsonPath.read[Any](lowerJson, path.toLowerCase)).map(_.toString).map { jsonValue =>
        if (filterValue.endsWith(".*")) {
          val regexPattern = Regex.quote(filterValue.dropRight(2)) + ".*"
          Try(jsonValue.matches(regexPattern)).getOrElse(false)
        } else {
          jsonValue == filterValue
        }
      }.getOrElse(false)
    }
  }
}

object JsonFilterRuleset {
  def apply(map: Map[String, String]): JsonFilterRuleset = {
    val rule = new JsonFilterRuleset()
    rule.rules = map
    rule
  }
}
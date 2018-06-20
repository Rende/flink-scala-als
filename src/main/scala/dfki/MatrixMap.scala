package dfki


import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.collection.mutable.HashMap

class MatrixMap extends RichFlatMapFunction[(String, Array[String], String), (Int, String, Int, String, Double)] {
  val logger = Logger(LoggerFactory.getLogger("MatrixMap"))

  @transient var esClient: ElasticService = _
  @transient var pairMap: HashMap[String, Int] = _
  @transient var aliasMap: HashMap[String, Int] = _
  @transient var relationMap: HashMap[String, Int] = _
  @transient var pairId: Int = 0
  @transient var relationSetId: Int = 0

  override def flatMap(tuple: (String, Array[String], String), out: Collector[(Int, String, Int, String, Double)]): Unit = {

    val pairId = generateIdForEntityPairs(tuple._1)
    // first insert aliases
    val aliases: Array[String] = esClient.getRelationAliases(tuple._3)
    fillAliasMap(aliases)
    for ((alias, id) <- aliasMap) {
      out.collect(pairId, tuple._1, id, alias, 1)
    }

    //insert relation phrase
    if (!tuple._2.isEmpty) {
      val key = appendPhrases(tuple._2)
      if (!key.isEmpty && !aliasMap.contains(key)) {
        out.collect(pairId, tuple._1, generateIdForRelation(key), key, 0.5)
      }
    }
  }

  def generateIdForEntityPairs(pair: String): Int = {
    if (pairMap.contains(pair)) {
      pairMap(pair)
    }
    else {
      pairId += 1
      pairMap += (pair -> pairId)
      pairId
    }
  }

  def generateIdForRelation(key: String): Int = {
    if (relationMap.contains(key)) {
      relationMap(key)
    } else {
      relationSetId += 1
      relationMap += key -> relationSetId
      relationSetId
    }
  }

  def fillAliasMap(aliasArray: Array[String]): Unit = {
    for (alias: String <- aliasArray) {
      val key = appendPhrases(alias.split(" "))
      if (!key.isEmpty && !aliasMap.contains(key)) {
        relationSetId += 1
        aliasMap += key -> relationSetId
      }
    }
  }

  def appendPhrases(relationArray: Array[String]): String = {
    val relationPhrases: Array[String] = relationArray.sortWith(_ > _)
    var key: String = ""
    for (relationPhrase: String <- relationPhrases) {
      key = relationPhrase + "_" + key
    }
    key.trim
  }

  override def open(config: Configuration): Unit = {
    esClient = new ElasticService()
    aliasMap = new HashMap[String, Int]
    pairMap = new HashMap[String, Int]
    relationMap = new HashMap[String, Int]
    pairId = 0
    relationSetId = 0
  }
}

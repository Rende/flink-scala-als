package dfki


import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.mutable.HashMap

class MatrixMap extends RichFlatMapFunction[(String, Array[String], String), (Int, String, Int, String, Double)] {
  @transient var esClient: ElasticService = _
  @transient var pairMap: HashMap[String, Int] = _
  @transient var relationMap: HashMap[String, (Int, Int)] = _
  @transient var pairId: Int = 0
  @transient var relationSetId: Int = 0

  override def flatMap(tuple: (String, Array[String], String), out: Collector[(Int, String, Int, String, Double)]): Unit = {

    val pairId = generateIdForEntityPairs(tuple._1)
    // first insert aliases
    val aliases: Array[String] = esClient.getRelationAliases(tuple._3)
    for (alias: String <- aliases) {
      if (!alias.isEmpty) {
        val (aliasId, key) = generateIdForRelation(alias.split(" "), pairId)
        if (aliasId != -1) {
          out.collect(pairId, tuple._1, aliasId, key, 1)
        }
      }
    }
    //then insert the relation phrases
    if (!tuple._2.isEmpty) {
      val (relationPhraseId, key) = generateIdForRelation(tuple._2, pairId)
      if (relationPhraseId != -1)
        out.collect(pairId, tuple._1, relationPhraseId, key, 0.5)
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

  def generateIdForRelation(relationArray: Array[String], pairId: Int): (Int, String) = {
    val relationPhrases: Array[String] = relationArray.sortWith(_ > _)
    var key: String = ""
    for (relationPhrase: String <- relationPhrases) {
      key = relationPhrase + "|" + key
    }
    key = key.trim
    if (!key.isEmpty) {
      if (relationMap.contains(key)) {
        val (prevPairId, relationId) = relationMap(key)
        if (prevPairId == pairId) (-1, "") else (relationId, key)

      } else {
        relationSetId += 1
        relationMap += key -> (pairId, relationSetId)
        (relationSetId, key)
      }
    }
    else {
      (-1, "")
    }
  }

  override def open(config: Configuration): Unit = {
    esClient = new ElasticService()
    pairMap = new HashMap[String, Int]
    relationMap = new HashMap[String, (Int, Int)]
    pairId = 0
    relationSetId = 0
  }
}

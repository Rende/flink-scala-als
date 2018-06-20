package dfki

import org.apache.flink.api.common.io.RichInputFormat
import org.apache.flink.api.common.io.statistics.BaseStatistics
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.io.InputSplitAssigner
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.searches.RichSearchResponse

import collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class ClusterDataSetInputFormat(splits: List[String]) extends RichInputFormat[Tuple3[String, Array[String], String], ClusterIdInputSplit] {

  private final val minClusterSize = 10000

  @transient lazy val esClient = new ElasticService()
  var searchResponse: RichSearchResponse = _
  var currentRecord = 0

  override def configure(parameters: Configuration): Unit = {}

  override def getStatistics(cachedStatistics: BaseStatistics): BaseStatistics = {
    null
  }

  override def createInputSplits(minNumSplits: Int): Array[ClusterIdInputSplit] = {
    val inputSplits = new ListBuffer[ClusterIdInputSplit]()
    var i: Int = 0
    for (split <- splits) {
      inputSplits.add(ClusterIdInputSplit(split, i))
      i += 1
    }
    inputSplits.toArray
  }

  override def getInputSplitAssigner(inputSplits: Array[ClusterIdInputSplit]): InputSplitAssigner = {
    val inputSplitAssigner: InputSplitAssigner = new ClusterInputSplitAssigner(inputSplits)
    inputSplitAssigner
  }

  override def open(split: ClusterIdInputSplit): Unit = {
    searchResponse = esClient.getClient.execute {
      search("cluster-entry-index/cluster-entries").matchQuery("cluster-id", split.getClusterId).scroll("1m").size(10000)
    }.await
  }

  override def reachedEnd(): Boolean = {
    searchResponse.hits.length == 0
  }

  override def nextRecord(reuse: (String, Array[String], String)): (String, Array[String], String) = {
    val subjectName: String = searchResponse.hits(currentRecord).sourceField("subj-name").toString
    val objectName: String = searchResponse.hits(currentRecord).sourceField("obj-name").toString
    val pair = subjectName + "|" + objectName
    var relationPhrase = searchResponse.hits(currentRecord).sourceField("relation-phrase-bow")
      .asInstanceOf[java.util.ArrayList[String]]
    val relationId: String = searchResponse.hits(currentRecord).sourceField("relation-id").toString

    if (isLastRecord) {
      scroll
      currentRecord = 0
    }
    else
      currentRecord += 1

    (pair, relationPhrase.asScala.toArray, relationId)

  }

  override def close(): Unit = {}

  def isLastRecord: Boolean =
    currentRecord == searchResponse.hits.length - 1

  def scroll: Unit = {
    searchResponse = esClient.getClient.execute(searchScroll(searchResponse.scrollId, "1m")).await
  }

  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 0
    }
  }

}
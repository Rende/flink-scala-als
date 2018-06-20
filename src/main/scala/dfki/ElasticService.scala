package dfki

import com.sksamuel.elastic4s.ElasticDsl.{search, termsAgg}
import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import org.elasticsearch.common.settings.Settings
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.http.search.Bucket
import com.sksamuel.elastic4s.searches.RichSearchResponse
import org.elasticsearch.search.aggregations.bucket.terms.Terms

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class ElasticService() {
  var client: TcpClient = _
  val settings = Settings.builder().put("cluster.name", "wiki-cluster").build()
  val minClusterSize: Int = 100

  def getClient: TcpClient = {
    if (client == null)
      client = TcpClient.transport(settings, ElasticsearchClientUri("elasticsearch://134.96.187.233:9300"))
    client
  }

  def getClusterIds: List[Terms.Bucket] = {
    val response = getClient.execute {
      search("cluster-entry-index/cluster-entries").matchAllQuery().aggs {
        termsAgg("clusters", "cluster-id").size(3000)
      }
    }.await

    val buckets = response.aggregations.termsResult("clusters").getBuckets
    val buffer = scala.collection.mutable.ListBuffer.empty[Terms.Bucket]
    for (bucket <- buckets) {
      if (bucket.getDocCount > minClusterSize) {
        buffer += bucket
      }
    }
    buffer.toList
  }

  def getRelationAliases(id: String): Array[String] = {

    val response = getClient.execute {
      search("wikidata-index/wikidata-entities").termQuery("_id", id).size(1)
    }.await

    var aliases: Array[String] = null
    if (isResponseValid(response)) {
      aliases = response.hits(0).sourceField("tok-aliases").asInstanceOf[java.util.ArrayList[String]].asScala.toArray
    }
    aliases
  }

  private def isResponseValid(response: RichSearchResponse) = response != null && response.totalHits > 0
}

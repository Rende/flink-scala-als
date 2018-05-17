package dfki

import com.sksamuel.elastic4s.ElasticDsl.{search, termsAgg}
import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import org.elasticsearch.common.settings.Settings
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.searches.RichSearchResponse

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class ElasticService() {
  var client: TcpClient = _
  val settings = Settings.builder().put("cluster.name", "wiki-cluster").build()

  def getClient():TcpClient = {
    if (client == null)
      client = TcpClient.transport(settings, ElasticsearchClientUri("elasticsearch://134.96.187.233:9300"))
    client
  }

  def getClusterIds() = {
    val response = getClient().execute {
      search("cluster-entry-index/cluster-entries").matchAllQuery().aggs {
        termsAgg("clusters", "cluster-id").size(3)
      }
    }.await

    response.aggregations.termsResult("clusters").getBuckets.toList
  }

  def getRelationAliases(id:String) ={

    val response = getClient().execute{
      search("wikidata-index/wikidata-entities").termQuery("_id",id).size(1)
    }.await

    var aliases: Array[String] = null
    if(isResponseValid(response)){
      aliases = response.hits(0).sourceField("tok-aliases").asInstanceOf[java.util.ArrayList[String]].asScala.toArray
    }
    aliases
  }

  private def isResponseValid(response: RichSearchResponse) = response != null && response.totalHits > 0
}

package dfki

import org.apache.flink.core.io.InputSplit

case class ClusterIdInputSplit (clusterId:String, splitNumber:Int) extends InputSplit{
  var getClusterId = clusterId
  override def getSplitNumber: Int = splitNumber
}

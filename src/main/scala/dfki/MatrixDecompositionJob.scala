package dfki


import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.util.Collector


object MatrixDecompositionJob {
  val esClient = new ElasticService()

  def main(args: Array[String]): Unit = {

    val clusters = esClient.getClusterIds()
    for (cluster <- clusters) {
      val env = ExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      val clusterId: String = cluster.getKey.toString
      val inputFormat = new ClusterDataSetInputFormat(List(clusterId))
      //Tuple from input => (pair, relationPhrase, relationId)
      val inputDS: DataSet[(String, String, String)] = env.createInput(inputFormat)
      val matrixDS = inputDS.flatMap[(String, String, Double)](
        new RichFlatMapFunction[(String, String, String), (String, String, Double)] {
          override def flatMap(tuple: (String, String, String), collector: Collector[(String, String, Double)]): Unit = {
            collector.collect(tuple._1, tuple._2, 0.5)
            val aliases = esClient.getRelationAliases(tuple._3)
            for (alias <- aliases)
              collector.collect(tuple._1, alias, 1.0)
          }
        })
      val als = ALS()
        .setIterations(10)
        .setNumFactors(10)
        .setBlocks(10)
        .setTemporaryPath("Users/aydanrende/Documents/temp")

      val parameters = ParameterMap()
        .add(ALS.Lambda, 0.0002)
        .add(ALS.Seed, 42L)

      als.fit(matrixDS, parameters)
      val predictedMatrix = als.predict(matrixDS)
      predictedMatrix.print()

      env.execute("MDJ")
    }
  }

}

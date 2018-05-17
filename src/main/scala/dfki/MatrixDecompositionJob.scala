package dfki


import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.util.Collector


object MatrixDecompositionJob {

  val esClient = new ElasticService()

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val clusters = esClient.getClusterIds()
    for (cluster <- clusters) {
      val clusterId: String = cluster.getKey.toString
      val inputFormat = new ClusterDataSetInputFormat(List(clusterId))
      val inputDS: DataSet[(String, Array[String], String)] = env.createInput(inputFormat)
      val middleDS = inputDS.flatMap[(Int, String, Int, String, Double)](new MatrixMap)
      env.setParallelism(1)
      middleDS.writeAsText("als_result/" + clusterId + ".txt", WriteMode.OVERWRITE)
      val matrixDS: DataSet[(Int, Int, Double)] = middleDS.map { triple => (triple._1, triple._3, triple._5) }
      val maxOfParameters = matrixDS.max(0).andMax(1)
      val testDS: DataSet[(Int, Int)] = maxOfParameters.flatMap(new RichFlatMapFunction[(Int, Int, Double), (Int, Int)] {
        override def flatMap(value: (Int, Int, Double), out: Collector[(Int, Int)]): Unit = {
          for (i: Int <- 1 to value._1) {
            for (j: Int <- 1 to value._2) {
              out.collect(i, j)
            }
          }
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

      val predictedMatrix = als.predict(testDS)
      env.setParallelism(1)
      predictedMatrix.writeAsText("als_result/" + clusterId + "_prediction.txt", WriteMode.OVERWRITE)
    }
    env.execute("MDJ")

  }

}

package dfki

import org.apache.flink.core.io.{InputSplit, InputSplitAssigner}




class ClusterInputSplitAssigner(inputSplits: Array[ClusterIdInputSplit]) extends InputSplitAssigner {
  /** The list of all splits */

  var splits = inputSplits


  override def getNextInputSplit(host: String, taskId: Int): InputSplit = {

    def next:ClusterIdInputSplit = synchronized {
      var split:ClusterIdInputSplit = null
      if (this.splits.size > 0) {
        split = this.splits.last
        println("Current Split "+ split.getClusterId + " and #clusters "+ this.splits.length)
        this.splits = this.splits.dropRight(1)
        println("#clusters " + this.splits.length)
      }
      split
    }
    next
  }
}

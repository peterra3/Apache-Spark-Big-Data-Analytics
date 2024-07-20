import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val accumulator = sc.longAccumulator("My Accumulator")  
    val textFile = sc.textFile(args(0))

    textFile.foreach { line =>
      val columns = line.split(",").tail
      val count = columns.count(rating => rating.nonEmpty)
      accumulator.add(count)
    }

    val totalRatings = accumulator.value

    val output = sc.parallelize(Seq(totalRatings.toString))
    output.saveAsTextFile(args(1))  

    sc.stop();
    }
}

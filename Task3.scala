import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val userRatingsCounts = textFile.flatMap { line =>
      val columns = line.split(",", -1).tail

      columns.zipWithIndex.map { case (columns, idx) =>
        (idx + 1, if (columns.nonEmpty) 1 else 0)
      }
    }
    .reduceByKey(_ + _)
    .collect()

    val output = sc.parallelize(userRatingsCounts.map { case (user, count) =>
      s"$user,$count"
    })

    output.saveAsTextFile(args(1))
    sc.stop()
  }
}

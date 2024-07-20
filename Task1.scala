import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    val movieRatings = textFile.map { line =>
      val columns = line.split(",")
      val movieTitle = columns.head
      val ratings = columns.tail.map { rating =>
        if (rating.isEmpty) 0 else rating.toInt
      }
      
      // Find maximum rating and corresponding user indices
      val maxRating = ratings.max
      val usersWithMaxRating = ratings.zipWithIndex
        .filter { case (rating, _) => rating == maxRating }
        .map { case (_, index) => index + 1 }
      
      (movieTitle, usersWithMaxRating)
    }
    val output = movieRatings.map { case (movieTitle, users) =>
      val usersList = if (users.isEmpty) "" else users.sorted.mkString(",")
      s"$movieTitle,$usersList"
    }
        
    output.saveAsTextFile(args(1))

    sc.stop()
  }
}

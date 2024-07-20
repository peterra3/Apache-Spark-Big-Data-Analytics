import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val moviesAndRatings = textFile.map { line =>
      val columns = line.split(",")
      val movieTitle = columns.head
      val ratings = columns.tail.map(rating => if (rating.isEmpty) None else Some(rating.toInt))
      (movieTitle, ratings)
    }

    // Cache the RDD to avoid recomputation
    val moviesAndRatingsCached = moviesAndRatings.cache()

    val cartesianProduct = moviesAndRatingsCached.cartesian(moviesAndRatingsCached)
    
    val moviePairs = cartesianProduct.mapPartitions(iter => {
      iter.filter { case ((movie1, _), (movie2, _)) =>
        movie1 < movie2
      }.map { case ((movie1, ratings1), (movie2, ratings2)) =>
        val commonRatingsCount = ratings1.zip(ratings2).count {
          case (Some(rating1), Some(rating2)) => rating1 == rating2
          case _ => false
        }
        (movie1, movie2, commonRatingsCount)
      }
    })

    val output = moviePairs.map { case (movie1, movie2, count) =>
      s"$movie1,$movie2,$count"
    }

    output.saveAsTextFile(args(1))
    moviesAndRatingsCached.unpersist()
    sc.stop()
  }
}

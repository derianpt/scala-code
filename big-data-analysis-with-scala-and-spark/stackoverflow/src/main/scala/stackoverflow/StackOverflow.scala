package stackoverflow

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    // First filter the questions and answers separately
    //then prepare them for a join operation by extracting the QID value in the first element of a tuple.
    val questionsRDD: RDD[(QID, Question)] = postings.filter(_.postingType == 1).map(question => (question.id, question))
    val answersRDD: RDD[(QID, Answer)] = postings.filter(_.postingType == 2).map(answer => (answer.parentId.get, answer))

    //Then, use one of the join operations (which one?) to obtain an RDD[(QID, (Question, Answer))].
    val questionsJoinAnswersRDD: RDD[(QID, (Question, Answer))] = questionsRDD.join(answersRDD)

    //Then, the last step is to obtain an RDD[(QID, Iterable[(Question, Answer)])].
    questionsJoinAnswersRDD.groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }

    // For each QID, pull out the iterable.
    // This RDD will now contain a set of mappings between all questions to its answers
    val discardedQID: RDD[Iterable[(Question, Answer)]] = grouped.values

    val answer: RDD[(Question, HighScore)] = discardedQID.map {
      // For each Iterable[(Question,Answer)], reduce it to a (Question, HighScore)
      iterable => {
        // create an Array[Answer] from this iterable
        val answerArray = iterable.map(qToATuple => qToATuple._2).toArray

        // get the answer high score.
        val highScoreOfQuestion = answerHighScore(answerArray)

        // take the question in this iterable
        val question = iterable.head._1

        // turn the whole iterable into a tuple
        (question, highScoreOfQuestion)
      }
    }
    answer
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    // for each tuple, find the lang of the question, calculate the lang index, and generate a new list of lang index to high score
    scored.map {
      case (question, highScore) => (firstLangInTag(question.tags, langs).get * langSpread, highScore)
    }
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(withReplacement = false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vects) => reservoirSampling(lang, vects.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    // For each vector, take the mean that is the closest to it and assign it.
    val meanToVectorRDD = vectors.map(vector => {
      val closestMeanIndex = findClosest(vector, means)
      (closestMeanIndex, vector)
    })

    // Calculate the average for each cluster and set them as the new list of means
    // Step 1: Sum up all vector x&y values in cluster, keeping track of total count of vectors
    val meanToClusterTotalRDD = meanToVectorRDD.aggregateByKey((0, 0, 0))(
      (first, second) => (first._1 + second._1, first._2 + second._2, first._3 + 1),
      (fromFirstCluster, fromSecondCluster) => (fromFirstCluster._1 + fromSecondCluster._1, fromFirstCluster._2 + fromSecondCluster._2, fromFirstCluster._3 + fromSecondCluster._3)
    )

    // Step 2: For each cluster, produce a new tuple that is the average of all x and y values of the vector.
    // and then UPDATE the index of the original mean. Either using mean(idx) = or mean.update(idx) =
    val newMeans = means.clone()
    meanToClusterTotalRDD.foreach {
      case (meanIndex, clusterTotals) =>
        val totalVectorsInCluster = clusterTotals._3
        newMeans(meanIndex) = (clusterTotals._1 / totalVectorsInCluster, clusterTotals._2 / totalVectorsInCluster)
    }

    println("originalMeans", means.length, means.mkString(","))
    println("newMeans", newMeans.length, newMeans.mkString(","))

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- means.indices)
        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
          f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double): Boolean =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the total euclidean distance between pairs of points in two arrays, with each pair being constructed
    * from the same index in both arrays.
    * This is used to calculate the shift between new means and old means.
    * */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length, s"original means is of size ${a1.length} while new means is of size ${a2.length}")
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- centers.indices) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>
      // Get label of the most popular language
      val langLabel: String = {
        val langIndex =
          vs.groupBy {
            case (langInd, _) => langInd
          }.toList.maxBy {
            case (_, vectorList) => vectorList.size
          }._1
        langs(langIndex / langSpread)
      }

      // Get percentage of the questions in the most common language
      val langPercent: Double = {
        val langIndexOfDominant = langs.indexOf(langLabel) * langSpread
        val numOfDominantLangQuestions = vs.count(tuple => tuple._1 == langIndexOfDominant)
        numOfDominantLangQuestions / vs.size
      }

      // Get size of cluster
      val clusterSize: Int = vs.size

      // Get median score in cluster
      val medianScore: Int = {
        val listOfScores = vs.map {
          case (_, highScore) => highScore
        }.toList.sorted
        listOfScores(listOfScores.length / 2)
      }

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"$score%7d  $lang%-17s ($percent%-5.1f%%)      $size%7d")
  }
}

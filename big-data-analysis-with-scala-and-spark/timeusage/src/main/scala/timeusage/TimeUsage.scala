package timeusage

import java.nio.file.Paths

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/** Main class */
object TimeUsage {

  import org.apache.spark.sql.SparkSession

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGrouped(summaryDf)
    finalDf.show()
  }

  /** @return The read DataFrame along with its column names. */
  def read(resource: String): (List[String], DataFrame) = {
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    val dataFrame =
      spark.createDataFrame(data, schema).persist()

    (headerColumns, dataFrame)
  }

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  /** @return The schema of the DataFrame, assuming that the first given column has type String and all the others
    *         have type Double. None of the fields are nullable.
    * @param columnNames Column names of the DataFrame
    */
  def dfSchema(columnNames: List[String]): StructType = {
    val firstColumnSchema = StructField(columnNames.head, StringType, nullable = false)
    val otherColumnsSchema = columnNames.tail.map(StructField(_, DoubleType, nullable = false))

    val schemaFields = firstColumnSchema :: otherColumnsSchema

    StructType(schemaFields)
  }


  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row = {
    val castedList: List[Any] = line.head :: line.tail.map(_.toDouble)
    Row(castedList: _*)
  }

  /** @return The initial data frame columns partitioned in three groups: primary needs (sleeping, eating, etc.),
    *         work and other (leisure activities)
    *
    * @see https://www.kaggle.com/bls/american-time-use-survey
    *
    * The dataset contains the daily time (in minutes) people spent in various activities. For instance, the column
    * “t010101” contains the time spent sleeping, the column “t110101” contains the time spent eating and drinking, etc.
    *
    * This method groups related columns together:
    * 1. “primary needs” activities (sleeping, eating, etc.). These are the columns starting with “t01”, “t03”, “t11”,
    *    “t1801” and “t1803”.
    * 2. working activities. These are the columns starting with “t05” and “t1805”.
    * 3. other activities (leisure). These are the columns starting with “t02”, “t04”, “t06”, “t07”, “t08”, “t09”,
    *    “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (those which are not part of the previous groups only).
    */
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {
    val primaryActColumnList = List("t01", "t03", "t11", "t1801", "t1803")
    val workingActColumnList = List("t05", "t1805")
    val otherActColumnList = List("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18")

    var primaryNeeds = new ListBuffer[Column]()
    var workingActivities = new ListBuffer[Column]()
    var otherActivities = new ListBuffer[Column]()

    columnNames.foreach {
      case primary if primaryActColumnList.exists(primary.startsWith) => primaryNeeds += $"$primary"
      case working if workingActColumnList.exists(working.startsWith) => workingActivities += $"$working"
      case other if otherActColumnList.exists(other.startsWith) => otherActivities += $"$other"
      case _ =>
    }
    (primaryNeeds.toList, workingActivities.toList, otherActivities.toList)
  }

  /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
    *         are summed together in a single column (and same for work and leisure). The “teage” column is also
    *         projected to three values: "young", "active", "elder".
    *
    * @param primaryNeedsColumns List of columns containing time spent on “primary needs”
    * @param workColumns List of columns containing time spent working
    * @param otherColumns List of columns containing time spent doing other activities
    * @param df DataFrame whose schema matches the given column lists
    *
    * This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
    * a single column.
    *
    * The resulting DataFrame should have the following columns:
    * - working: value computed from the “telfs” column of the given DataFrame:
    *   - "working" if 1 <= telfs < 3
    *   - "not working" otherwise
    * - sex: value computed from the “tesex” column of the given DataFrame:
    *   - "male" if tesex = 1, "female" otherwise
    * - age: value computed from the “teage” column of the given DataFrame:
    *   - "young" if 15 <= teage <= 22,
    *   - "active" if 23 <= teage <= 55,
    *   - "elder" otherwise
    * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
    * - work: sum of all the `workColumns`, in hours
    * - other: sum of all the `otherColumns`, in hours
    *
    * Finally, the resulting DataFrame should exclude people that are not employable (ie telfs = 5).
    *
    * Note that the initial DataFrame contains time in ''minutes''. You have to convert it into ''hours''.
    */
  def timeUsageSummary(
    primaryNeedsColumns: List[Column],
    workColumns: List[Column],
    otherColumns: List[Column],
    df: DataFrame
  ): DataFrame = {
    // Transform the data from the initial dataset into data that make
    // more sense for our use case
    // Hint: you can use the `when` and `otherwise` Spark functions
    // Hint: don’t forget to give your columns the expected name with the `as` method
    val telfsColumn = $"telfs"
    val workingStatusProjection: Column = when(telfsColumn >= 1 && telfsColumn < 3, "working")
      .otherwise("not working").as("working")

    val tesexColumn = $"tesex"
    val sexProjection: Column = when(tesexColumn === 1, "male").otherwise("female").as("sex")

    val teageColumn = $"teage"
    val ageProjection: Column = when(teageColumn >= 15 && teageColumn <= 22, "young")
      .when(teageColumn >= 23 && teageColumn <= 55, "active")
      .otherwise("elder").as("age")

    // Create columns that sum columns of the initial dataset
    // Hint: you want to create a complex column expression that sums other columns
    //       by using the `+` operator between them
    // Hint: don’t forget to convert the value to hours
    val primaryNeedsProjection: Column = primaryNeedsColumns.reduce(_ + _).divide(60).as("primaryNeeds")
    val workProjection: Column = workColumns.reduce(_ + _).divide(60).as("work")
    val otherProjection: Column = otherColumns.reduce(_ + _).divide(60).as("other")

    val updatedDf = df.withColumn("working", workingStatusProjection)
      .withColumn("sex", sexProjection)
      .withColumn("age", ageProjection)
      .withColumn("primaryNeeds", primaryNeedsProjection)
      .withColumn("work", workProjection)
      .withColumn("other", otherProjection)

    val projectedDf = updatedDf
      .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
      .where($"telfs" <= 4).persist() // Discard people who are not in labor force

    projectedDf
  }

  /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
    *         ages of life (young, active or elder), sex and working status.
    * @param summed DataFrame returned by `timeUsageSumByClass`
    *
    * The resulting DataFrame should have the following columns:
    * - working: the “working” column of the `summed` DataFrame,
    * - sex: the “sex” column of the `summed` DataFrame,
    * - age: the “age” column of the `summed` DataFrame,
    * - primaryNeeds: the average value of the “primaryNeeds” columns of all the people that have the same working
    *   status, sex and age, rounded with a scale of 1 (using the `round` function),
    * - work: the average value of the “work” columns of all the people that have the same working status, sex
    *   and age, rounded with a scale of 1 (using the `round` function),
    * - other: the average value of the “other” columns all the people that have the same working status, sex and
    *   age, rounded with a scale of 1 (using the `round` function).
    *
    * Finally, the resulting DataFrame should be sorted by working status, sex and age.
    */
  def timeUsageGrouped(summed: DataFrame): DataFrame = {
    summed
      .groupBy($"working", $"sex", $"age")
      .avg()
      .withColumn("primaryNeeds", round($"avg(primaryNeeds)", 1))
      .withColumn("work", round($"avg(work)", 1))
      .withColumn("other", round($"avg(other)", 1))
      .sort("working", "sex", "age")
      .select("working", "sex", "age", "primaryNeeds", "work", "other")
  }

  /**
    * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
    * @param summed DataFrame returned by `timeUsageSumByClass`
    */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(timeUsageGroupedSqlQuery(viewName))
  }

  /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
    * @param viewName Name of the SQL view to use
    */
  def timeUsageGroupedSqlQuery(viewName: String): String = {
    s"""
       |SELECT working, sex, age, round(AVG(primaryNeeds),1) AS primaryNeeds, round(AVG(work),1) AS work, round(AVG(other),1) AS other
       |FROM $viewName
       |GROUP BY working, sex, age
       |ORDER BY working, sex, age
     """.stripMargin
  }

  /**
    * @return A `Dataset[TimeUsageRow]` from the “untyped” `DataFrame`
    * @param timeUsageSummaryDf `DataFrame` returned by the `timeUsageSummary` method
    *
    * Hint: you should use the `getAs` method of `Row` to look up columns and
    * cast them at the same time.
    */
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] = {
    timeUsageSummaryDf.map(row => TimeUsageRow(
      row.getAs[String]("working"),
      row.getAs[String]("sex"),
      row.getAs[String]("age"),
      row.getAs[Double]("primaryNeeds"),
      row.getAs[Double]("work"),
      row.getAs[Double]("other")
    ))
  }

  /**
    * @return Same as `timeUsageGrouped`, but using the typed API when possible
    * @param summed Dataset returned by the `timeUsageSummaryTyped` method
    *
    * Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
    * dataset contains one element per respondent, whereas the resulting dataset
    * contains one element per group (whose time spent on each activity kind has
    * been aggregated).
    *
    * Hint: you should use the `groupByKey` and `typed.avg` methods.
    */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    summed
      .groupByKey(row => (row.working, row.sex, row.age))
      .agg(
        round(avg($"primaryNeeds"), 1).as("primaryNeeds").as[Double],
        round(avg($"work"), 1).as("work").as[Double],
        round(avg($"other"), 1).as("other").as[Double]
      )
      .map {
        case (keys, primaryNeeds, work, other) =>
          TimeUsageRow(keys._1, keys._2, keys._3, primaryNeeds, work, other)
      }
      .orderBy("working", "sex", "age")
  }
}

/**
  * Models a row of the summarized data set
  * @param working Working status (either "working" or "not working")
  * @param sex Sex (either "male" or "female")
  * @param age Age (either "young", "active" or "elder")
  * @param primaryNeeds Number of daily hours spent on primary needs
  * @param work Number of daily hours spent on work
  * @param other Number of daily hours spent on other activities
  */
case class TimeUsageRow(
  working: String,
  sex: String,
  age: String,
  primaryNeeds: Double,
  work: Double,
  other: Double
)

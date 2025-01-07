import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IncrementalLoad {
  def main(args: Array[String]): Unit = {

    // Initialize Spark session
    val spark = SparkSession.builder()
      .master("local")
      .appName("IncrementalLoad")
      .enableHiveSupport()
      .getOrCreate()
//ok
    // Step 1: Set the last Cumulative_Volume value manually (this can be dynamic in your production environment)
    val lastCumulativeVolume = 36805900.826118246
    println(s"Max Cumulative_Volume: $lastCumulativeVolume")  // Print the last Cumulative_Volume value

    // Step 2: Build the query to get data from PostgreSQL where Cumulative_Volume > last Cumulative_Volume
    val query = s"""SELECT * FROM bitcoin_2025 WHERE "Cumulative_Volume" > $lastCumulativeVolume"""

    // Step 3: Read data from PostgreSQL using the query
    val newData = spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb")
      .option("driver", "org.postgresql.Driver")
      .option("user", "consultants")
      .option("password", "WelcomeItc@2022")
      .option("query", query)
      .load()

    // Show the new data that was loaded
    newData.show()

    // Step 4: Write the new data to Hive
    newData.write.mode("append").saveAsTable("project2024.bitcoin_inc_team")

    println("Successfully loaded data into Hive")

    // Optionally: Check for further transformations or actions
    // For example, if you want to join with other data, you can do so as follows:
    // val df2 = spark.read.option("header", "true").option("inferSchema", "true").csv("path/to/other_file.csv")
    // val joinedDF = newData.join(df2, Seq("ID"), "inner")
    // joinedDF.show()
  }
}

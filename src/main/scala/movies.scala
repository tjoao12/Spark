object movies extends App {

  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.log4j._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf1 = new SparkConf()
  conf1.setAppName("main")
  conf1.setMaster("local[*]")
  val sc = new SparkContext(conf1)
  val sqlContext = new SQLContext(sc)
  val rddFile = sc.textFile("C:\\myfriend\\spark\\data.txt")
  val rddFile2 = sc.textFile("C:\\myfriend\\spark\\data2.txt")

  //function for the first text file
  def splitElements(records: String): Tuple4[String, String, Int, String] = {
    val elements = records.split("\t")
    (elements(0), elements(1), elements(2).toInt, elements(3))
  }
  //function for the second text file
  def splitElements2(records: String): Tuple2[String, String] = {
    val elements = records.split("\\|")
    (elements(0), elements(1))
  }
  //data frame for the first file
  var Rdd1 = rddFile.map(splitElements)
  var Rdd2 = Rdd1.filter(x => x._3 == 5)
  val df1 = sqlContext.createDataFrame(Rdd2).toDF("User ID", "Movie ID", "Rating", "Time Stamp")
  var df2 = df1.groupBy("Movie ID").count
  //data frame for the second file
  var Rdd3 = rddFile2.map(splitElements2)
  var df3 = df2.orderBy(desc("count"))
  var df4 = df3.select("Movie ID")
  var df5 = df4.first()
  var df6 = Rdd3.filter(x => x._1 == df5(0))
  val df7 = sqlContext.createDataFrame(df6).toDF("Movie ID", "Film name")
  df7.show()
}
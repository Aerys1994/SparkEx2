package sparkSession

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {

  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder().master("local")
      .appName(appName)
      //.config("spark.sql.catalogImplementation", "hive")
      .enableHiveSupport()
      .getOrCreate()
  }

}

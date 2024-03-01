import blocks.Block1
import sparkSession.SparkSessionProvider

import scala.blocks.{Block2, Block3, Block6}

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSessionProvider.createSparkSession("Spark")
    spark.sparkContext.setLogLevel("ERROR")

    //Block1.execute(spark)
    //Block2.execute(spark)
    //Block3.execute(spark)
    Block6.execute(spark)

    spark.stop()

  }

}
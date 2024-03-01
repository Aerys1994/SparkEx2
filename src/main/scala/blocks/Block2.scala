package scala.blocks

import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.sys.process._


object Block2 {

  def execute(spark: SparkSession): Unit = {

    //Ex 2.1
    val padronTxtDF = spark.table("datos_padron_txt")

    padronTxtDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("padron_parquet")

    //Ex 2.2
    val padronTxt2DF = spark.table("padron_txt_2")


    padronTxt2DF.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .saveAsTable("padron_parquet_2")

    //Ex 2.6
   /* val tableNames = Seq("datos_padron_txt", "padron_txt_2", "padron_parquet", "padron_parquet_2")

    var padronTxtLocation = ""
    var padronTxt2Location = ""
    var padronParquetLocation = ""
    var padronParquet2Location = ""

    for (currentTableName <- tableNames) {
      val showCreateTableResult = spark.sql(s"SHOW CREATE TABLE $currentTableName").collect()
      println(s"SHOW CREATE TABLE result for $currentTableName:")
      showCreateTableResult.foreach(println)

      val tableName = currentTableName // Reemplaza con el nombre de la tabla correspondiente
      val locationDF = spark.sql(s"DESCRIBE FORMATTED $tableName")
      val tableLocationRow = locationDF.filter(locationDF("col_name") === "Location").select("data_type").collect()(0)
      val tableLocation = tableLocationRow.getString(0)
      val locationParts = tableLocation.split(" ")
      println(s"Table location parts for $tableName:")
      locationParts.foreach(println)

      currentTableName match {
        case "datos_padron_txt" => padronTxtLocation = locationParts.last
        case "padron_txt_2" => padronTxt2Location = locationParts.last
        case "padron_parquet" => padronParquetLocation = locationParts.last
        case "padron_parquet_2" => padronParquet2Location = locationParts.last
      }
    }

    // Función para obtener el tamaño de un directorio en HDFS
    def getDirectorySize(location: String): String = {
      s"hadoop fs -du -h $location".!!.split("\\s+").head
    }

    // Obtener el tamaño de los directorios de cada tabla
    val padronTxtSize = getDirectorySize(padronTxtLocation)
    val padronTxt2Size = getDirectorySize(padronTxt2Location)
    val padronParquetSize = getDirectorySize(padronParquetLocation)
    val padronParquet2Size = getDirectorySize(padronParquet2Location)

    // Imprimir los resultados
    println(s"Tamaño de padron_txt: $padronTxtSize")
    println(s"Tamaño de padron_txt_2: $padronTxt2Size")
    println(s"Tamaño de padron_parquet: $padronParquetSize")
    println(s"Tamaño de padron_parquet_2: $padronParquet2Size")*/
  }

}
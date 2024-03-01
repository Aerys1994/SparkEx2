package blocks

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._


object Block1 {

  def execute(spark: SparkSession): Unit = {

    //Ex 1.1
    spark.sql("CREATE DATABASE IF NOT EXISTS datos_padron")

    val data = spark.read.option("header", "true").option("sep", ";").csv("src/main/resources/estadisticas202402.csv")


    data.write.mode("overwrite").saveAsTable("datos_padron_txt")

    println("Tabla 'datos_padron_txt' sobrescrita exitosamente.")


    spark.sql("SHOW TABLES").show()

    val tabla = "datos_padron_txt"
    val resultado = spark.sql(s"SELECT * FROM $tabla LIMIT 10")
    resultado.show()

    //Ex 1.3
    val trimmedData = data.select(
      data.columns.map(c => trim(col(c)).alias(c)): _*
    )

    trimmedData.write.mode("overwrite").saveAsTable("padron_txt_2")

    spark.sql("SHOW TABLES").show()

    //Ex 1.5
    val cleanedPadronTxtDF = resultado.select(
      col("COD_DISTRITO"),
      col("DESC_DISTRITO"),
      col("COD_DIST_BARRIO"),
      col("DESC_BARRIO"),
      col("COD_BARRIO"),
      col("COD_DIST_SECCION"),
      col("COD_SECCION"),
      col("COD_EDAD_INT"),
      when(length(col("ESPANOLESHOMBRES")) === 0, lit(0)).otherwise(col("ESPANOLESHOMBRES")).as("ESPANOLESHOMBRES"),
      when(length(col("ESPANOLESMUJERES")) === 0, lit(0)).otherwise(col("ESPANOLESMUJERES")).as("ESPANOLESMUJERES"),
      when(length(col("EXTRANJEROSHOMBRES")) === 0, lit(0)).otherwise(col("EXTRANJEROSHOMBRES")).as("EXTRANJEROSHOMBRES"),
      when(length(col("EXTRANJEROSMUJERES")) === 0, lit(0)).otherwise(col("EXTRANJEROSMUJERES")).as("EXTRANJEROSMUJERES"),
      when(length(col("FX_CARGA")) === 0, lit(0)).otherwise(col("FX_CARGA")).as("FX_CARGA"),
      when(length(col("FX_DATOS_INI")) === 0, lit(0)).otherwise(col("FX_DATOS_INI")).as("FX_DATOS_INI"),
      when(length(col("FX_DATOS_FIN")) === 0, lit(0)).otherwise(col("FX_DATOS_FIN")).as("FX_DATOS_FIN")
    )

    cleanedPadronTxtDF.write.mode("overwrite").saveAsTable("padron_txt_clean")

    cleanedPadronTxtDF.show()

    //Ex 1.6



  }

}

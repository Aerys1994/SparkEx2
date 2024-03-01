package scala.blocks


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.expressions.Window

object Block6 {

  def execute(spark: SparkSession): Unit = {

    //Ex 6.1 & 6.2
    val schema = "COD_DISTRITO INT, DESC_DISTRITO STRING, COD_DIST_BARRIO INT, DESC_BARRIO STRING, " +
      "COD_BARRIO INT, COD_DIST_SECCION INT, COD_SECCION INT, COD_EDAD_INT STRING, " +
      "ESPANOLESHOMBRES INT, ESPANOLESMUJERES INT, EXTRANJEROSHOMBRES INT, " +
      "EXTRANJEROSMUJERES INT, FX_CARGA STRING, FX_DATOS_INI STRING, FX_DATOS_FIN STRING"

    val dfRaw = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("quote", "\"")
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .schema(schema)
      .csv("src/main/resources/estadisticas202402.csv")

    val dfStringFilled = dfRaw.na.fill("0", dfRaw.columns.filter(col => dfRaw.schema(col).dataType == org.apache.spark.sql.types.StringType))
    val df = dfStringFilled.na.fill(0)

    //Ex 6.3
    val barriosDistinct = df.select("DESC_BARRIO").distinct().withColumnRenamed("DESC_BARRIO", "Barrio")
    println("Barrios distintos")
    barriosDistinct.show()

    //Ex 6.4
    df.createOrReplaceTempView("padron")

    val numBarriosDistinct = spark.sql("SELECT COUNT(DISTINCT DESC_BARRIO) AS num_barrios FROM padron")

    println("Temp Barrios distintos")
    numBarriosDistinct.show()

    //Ex 6.5
    val dfConLongitud = df.withColumn("longitud", functions.length(col("DESC_DISTRITO")))
    println("Longitud añadida")
    dfConLongitud.show()

    //Ex 6.6
    val dfConNuevoValor = df.withColumn("valor_5", lit(5))
    println("Valor 5")
    dfConNuevoValor.show()

    //Ex 6.7
    val dfSinNuevoValor = dfConNuevoValor.drop("valor_5")
    println("Columna eliminada")
    dfSinNuevoValor.show()

    //Ex 6.8
    val dfPartitioned = df.repartitionByRange(col("DESC_DISTRITO"), col("DESC_BARRIO"))
    println("df particionado")
    dfPartitioned.show()

    //Ex 6.9
    dfPartitioned.cache()

    //Ex 6.10
    val consulta = dfPartitioned.groupBy("DESC_DISTRITO", "DESC_BARRIO")
      .agg(
        sum("ESPANOLESHOMBRES").alias("total_espanoleshombres"),
        sum("ESPANOLESMUJERES").alias("total_espanolesmujeres"),
        sum("EXTRANJEROSHOMBRES").alias("total_extranjeroshombres"),
        sum("EXTRANJEROSMUJERES").alias("total_extranjerosmujeres")
      )
      .orderBy(
        col("total_extranjerosmujeres").desc,
        col("total_extranjeroshombres").desc
      )
    println("Agrupación")
    consulta.show()

    //Ex 6.11
    dfPartitioned.unpersist()

    //Ex 6.12
    val dfTotalEspanolesHombres = dfPartitioned.groupBy("DESC_BARRIO", "DESC_DISTRITO")
      .agg(sum("ESPANOLESHOMBRES").alias("total_espanoleshombres"))

    val dfUnido = dfPartitioned.join(dfTotalEspanolesHombres,
      Seq("DESC_BARRIO", "DESC_DISTRITO"), "inner")

    dfUnido.show()

    //Ex 6.13
    val ventana = Window.partitionBy("DESC_BARRIO", "DESC_DISTRITO")

    val dfEspanolesHombres = dfPartitioned.withColumn("total_espanoleshombres",
      sum("ESPANOLESHOMBRES").over(ventana))

    println("Españoles hombre")
    dfEspanolesHombres.show()

    //Ex 6.14
    val dfFiltrado = dfPartitioned
      .filter(trim(col("DESC_DISTRITO")) === "CENTRO" ||
        trim(col("DESC_DISTRITO")) === "BARAJAS" ||
        trim(col("DESC_DISTRITO")) === "RETIRO")
      .select(
        col("DESC_DISTRITO"),
        col("COD_EDAD_INT").cast("integer").alias("COD_EDAD_INT"),
        col("ESPANOLESMUJERES"))

    val pivoted_df = dfFiltrado.groupBy("COD_EDAD_INT")
      .pivot("DESC_DISTRITO")
      .agg(sum("ESPANOLESMUJERES"))
      .orderBy("COD_EDAD_INT")

    pivoted_df.show()
    //pivoted_df.printSchema()

    //Ex 6.15
    val pivoted_df_cleaned = pivoted_df.select(
      col("COD_EDAD_INT"),
      col("BARAJAS             ").as("BARAJAS"),
      col("CENTRO              ").as("CENTRO"),
      col("RETIRO              ").as("RETIRO")
    )

    val df_con_porcentajes = pivoted_df_cleaned.withColumn("Porcentaje_BARAJAS", col("BARAJAS") * 100 / (col("BARAJAS") + col("CENTRO") + col("RETIRO")))
      .withColumn("Porcentaje_CENTRO", col("CENTRO") * 100 / (col("BARAJAS") + col("CENTRO") + col("RETIRO")))
      .withColumn("Porcentaje_RETIRO", col("RETIRO") * 100 / (col("BARAJAS") + col("CENTRO") + col("RETIRO")))

    df_con_porcentajes.show()

    //Ex 6.16
    val outputPath = "src/main/resources/saves"

    dfPartitioned.write
      .mode("overwrite")
      .partitionBy("DESC_DISTRITO", "DESC_BARRIO").csv(outputPath)



  }

}

package net.sparktanos

import net.sparktanos.Main.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType, DateType}

class Ingestion (
                  pathInput: String,
                  schema: String,
                  table: String,
                  parition: String,
                  descPartition: String,
                  projectId: String,
                  frecuency: String,
                ){

  /**
   * Globals Variables
   */
  var delimiter:String = "|"
  val tmpBucketGSC:String = "bdb-gcp-qa-cds-storage-dataproc"

  /**
   * Globals Dataframes
   */
  var dfSource:DataFrame = spark.emptyDataFrame
  var dfTableLocation:DataFrame = spark.emptyDataFrame

  def initProcess (): Unit={
    println("[START] Into function InitProcess IngestionJob")
    println(s"s[START] Load data table of: ${pathInput}")

    dfSource = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", delimiter)
      .option("header", "true")
      .option("quoteMode", "NONE")
      .option("charset","UTF-8")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue", "null")
      .option("timestampFormat", "yyyy-MM-dd hh:mm:ss")
      .option("mode", "PERMISSIVE")
      .load(pathInput)
    println(s"The numbers rows are: ${dfSource.count()}")
    println(s"The number colums are: ${dfSource.columns.length}")

    //dfSource = dfSource.withColumn("fecha_cargue", to_date(concat(col("toDate")), "yyyyMMdd"))
    //dfSource = dfSource.withColumn("fecha_cargue", dfSource.col("fecha_cargue").cast(Date))
    //dfSource = dfSource.withColumn("fecha_cargue", date_format(current_timestamp(), "yyyyMMdd"))
    dfSource = dfSource.withColumn("partitionDaily", to_date(col("fecha_cargue").cast("string"), "yyyyMMdd"))
    dfSource.show(5)
    dfSource.printSchema()

    println(s"s[END] Load data table of: ${table}")

    println(s"s[START] Write data table in BQ Without partitioned")
    dfSource.write
      .format("bigquery")
      .mode("append")
      .option("temporaryGcsBucket", tmpBucketGSC)
      .option("partitionField", "partitionDaily")
      .option("partitionType", "DAY")
      .option("table", schema + "." + table) //customers_dataset.customers_output  --> esta opcion quedara obsoleta a futuro
      .save()
    println(s"s[END] Write data table in BQ")

    //.option("partitionField", "partitionDaily")
    //.option("partitionType", "MONTH") -- solo es concepto en BQ -> para las tablas mensuales
    //.option("project", "bigquery-project-id")
    //.save("dataset.table") --> esta sera la version de escribir en BQ





}


}

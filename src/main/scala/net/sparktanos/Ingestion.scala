package net.sparktanos

import net.sparktanos.Main.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

class Ingestion (
                  schema: String,
                  table: String,
                  parition: String,
                  descPartition: String

                ){

  /**
   * Globals Variables
   */
  var projectId:String = ""
  var datasetBQ:String = "maestro"
  var tableBQ:String = "BDB_CUSTOMERS"
  var delimiter:String = "|"
  val tmpBucketGSC:String = "bdb-gcp-cds-storage-dataproc"

  /**
   * Globals Dataframes
   */
  var dfSource:DataFrame = spark.emptyDataFrame
  var dfTableLocation:DataFrame = spark.emptyDataFrame

  def initProcess (): Unit={
    println("[START] Into function InitProcess IngestionJob")
    println(s"s[START] Load data table of: ${table}")

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
      .load(table)
    println(s"The numbers rows are: ${dfSource.count()}")
    println(s"The number colums are: ${dfSource.columns.length}")
    dfSource.printSchema()

    println(s"s[END] Load data table of: ${table}")

    println(s"s[START] Write data table in BQ Without partitioned")
    dfSource.write
      .format("bigquery")
      .mode("overwrite")
      .option("temporaryGcsBucket", tmpBucketGSC)
      .option("table", datasetBQ + "." + tableBQ) //customers_dataset.customers_output
      .save()
    println(s"s[END] Write data table in BQ")







}


}

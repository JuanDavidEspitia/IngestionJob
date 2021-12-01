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
  var pathInput:String = ""
  var pathOutput:String = ""
  var delimiter:String = "|"

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







}


}

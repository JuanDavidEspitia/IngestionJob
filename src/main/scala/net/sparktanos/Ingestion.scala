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
    dfSource.show(5)
    dfSource.printSchema()
    println(s"s[END] Load data table of: ${table}")


    if(frecuency == "D"){
      println("The Frecuency table is DAILY")

      //dfSource = dfSource.withColumn("fecha_cargue", to_date(concat(col("toDate")), "yyyyMMdd"))
      //dfSource = dfSource.withColumn("fecha_cargue", dfSource.col("fecha_cargue").cast(Date))
      //dfSource = dfSource.withColumn("fecha_cargue", date_format(current_timestamp(), "yyyyMMdd"))
      dfSource = dfSource.withColumn("partition", to_date(col("fecha_cargue").cast("string"), "yyyyMMdd"))


      println(s"s[START] Write data table in BQ With partition")
      dfSource.write
        .format("bigquery")
        .mode("append")
        .option("project", projectId)
        .option("temporaryGcsBucket", tmpBucketGSC)
        .option("partitionField", "partition")
        .option("clusteredFields", "id_type,city")
        .option("partitionType", "DAY")
        .option("table", schema + "." + table) //customers_dataset.customers_output  --> esta opcion quedara obsoleta a futuro
        .save()
      println(s"s[END] Write data table in BQ")
      //.option("partitionField", "partitionDaily")
      //.option("partitionType", "MONTH") -- solo es concepto en BQ -> para las tablas mensuales
      //.option("project", "bigquery-project-id")
      //.option("datePartition", "YYYYMMDD")
      //.save("dataset.table") --> esta sera la version de escribir en BQ



    }else if(frecuency =="M"){
      println("The Frecuency table is MONTH")

      dfSource = dfSource.withColumn("partition", to_date(col("fecha_cargue").cast("string"), "yyyyMM"))

      println(s"s[START] Write data table in BQ With partition")
      dfSource.write
        .format("bigquery")
        .mode("append")
        .option("project", projectId)
        .option("temporaryGcsBucket", tmpBucketGSC)
        .option("partitionField", "partition")
        .option("partitionType", "MONTH")
        .option("table", schema + "." + table) //customers_dataset.customers_output  --> esta opcion quedara obsoleta a futuro
        .save()
      println(s"s[END] Write data table in BQ")




    }else{
      println("The frecuency is not define, please verify argument frecuency")
      spark.stop()
    }

    println("[START]  Load Data from BQ by QUERY SQL")
    var sql = s"""
              SELECT table_catalog, table_schema, table_name, partition_id
              FROM `$projectId.$schema.INFORMATION_SCHEMA.PARTITIONS`
              WHERE table_name = '$table'
              """
    val df = spark.read
      .format("bigquery")
      .option("viewsEnabled","true")
      .option("materializationDataset", schema)
      .option("query", sql)
      .load()
      .cache()
    df.show()
    spark.stop()




    /* leer data de BQ con filtros
    val df = spark.read.format("bigquery")
      .option("table", "bigquery-public-data.samples.github_nested")
      .load()
      .where("payload.pull_request.user.id > 500 and repository.url='https://github.com/bitcoin/bitcoin'")
      .select("payload.pull_request.user.url")
      .distinct
      .as[String]
      .sort("payload.pull_request.user.url")
      .take(3)
     */


    /*
    // Reading Data from Bigquery by SQL Sintax
    println("[START Load dataframe by SQL Sintaxt")
    spark.conf.set("viewsEnabled","true")
    spark.conf.set("materializationDataset", schema)

    var sql = """
      SELECT tag, COUNT(*) c
      FROM (
        SELECT SPLIT(tags, '|') tags
        FROM `bigquery-public-data.stackoverflow.posts_questions` a
        WHERE EXTRACT(YEAR FROM creation_date)>=2014
      ), UNNEST(tags) tag
      GROUP BY 1
      ORDER BY 2 DESC
      LIMIT 10
      """
    var df = spark.read.format("bigquery").load(sql)
    df.show()

    var sql2 = s"""
     SELECT table_catalog, table_schema, table_name, partition_id FROM `$projectId.$schema.INFORMATION_SCHEMA.PARTITIONS` WHERE table_name = '$table'
        """
    var df2 = spark.read.format("bigquery").load(sql2)
    df2.show()



 */




}


}

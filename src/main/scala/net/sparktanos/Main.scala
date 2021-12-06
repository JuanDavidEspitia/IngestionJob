package net.sparktanos

import org.apache.spark.sql.SparkSession
import scopt.OptionParser

case class Config (
                    pathInput: String = null,
                    schema: String = null,
                    table: String = null,
                    partition: String = null,
                    descPartition: String = null,
                    projectId: String = null,
                    frecuency: String = null
                  )

object Main {

  val nameApp:String = "ExportJob"
  var pathInput: String= null
  var schema: String = null
  var table: String = null
  var partition: String = null
  var descPartition: String = null
  var projectId: String = null
  var frecuency: String = null

  val spark = SparkSession
    .builder
    .appName(nameApp)
    .getOrCreate()


  def main(args: Array[String]): Unit = {

    if (args.length > 0){
      println("[START] Scopt Parser Arguments")
      val parser = new OptionParser[Config]("ExportJob") {
        head("scopt", "1.0", "Allow read Arguments")
        opt[String]('b', "pathInput")
          .required()
          .action((x,c) => c.copy(pathInput = x))
          .text("Path of file CSV")
        opt[String]('s', "schema")
          .required()
          .action((x,c) => c.copy(schema = x))
          .text("schema o data base of the table")
        opt[String]('t', "table")
          .required()
          .action((x,c) => c.copy(table = x))
          .text("table to write in BQ")
        opt[String]('p', "partition")
          .required()
          .action((x,c) => c.copy(partition = x))
          .text("partition table")
        opt[String]('d', "descPartition")
          .action((x,c) => c.copy(descPartition = x))
          .text("description of partition table")
        opt[String]('i', "projectId")
          .action((x,c) => c.copy(projectId = x))
          .text("The Google Cloud Project ID of the table")
        opt[String]('f', "frecuency")
          .required()
          .action((x,c) => c.copy(frecuency = x))
          .text("Frecuecy extraccion of tables")
        help("help").text("print this usage text")
      }

      parser.parse(args, Config()) map { config =>
        pathInput = config.pathInput
        schema = config.schema
        table = config.table
        partition = config.partition
        descPartition = config.descPartition
        projectId = config.projectId
        frecuency = config.frecuency

        println("[END] Scopt Parser Arguments")

        val arg: String = "Arguments used are: \n- " +
          "[Required] Path Input: " + pathInput + "\n- " +
          "[Required] Schema: " + schema + "\n- " +
          "[Required] Table: " + table + "\n- " +
          "[Required] Partition: " + partition + "\n- " +
          "[Optional] Description Partition: " + descPartition + "\n- " +
          "[Optional] ID Project: " + projectId + "\n- " +
          "[Required] Frecuency Extraction: " + frecuency + "\n- "

        //showing args
        println(arg)

        println("[START] Process Ingestion Job")
        val ingestion = new Ingestion(pathInput, schema, table, partition, descPartition, projectId, frecuency)
        ingestion.initProcess()
        println("[END] Finish Process")
        spark.stop()

      } getOrElse {
        println("[END] Arguments are badly define")
        spark.stop()
      }

    }else {
      println("[END] There are not arguments to process")
      spark.stop()
    }


  }


}
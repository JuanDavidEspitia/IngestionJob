package net.sparktanos

import org.apache.spark.sql.SparkSession
import scopt.OptionParser

case class Config (
                    schema: String = null,
                    table: String = null,
                    parition: String = null,
                    descPartition: String = null
                  )

object Main {

  val nameApp:String = "ExportJob"
  var schema: String = null
  var table: String = null
  var parition: String = null
  var descPartition: String = null
  val spark = SparkSession
    .builder
    .appName(nameApp)
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    if (args.length > 0){
      println("[START] Scopt Parser Arguments")
      val parser = new OptionParser[Config]("ExportJob") {
        head("scopt", "1.0", "Allow read Arguments")
        opt[String]('s', "schema")
          .required()
          .action((x,c) => c.copy(schema = x))
          .text("schema o data base of the table")
        opt[String]('t', "table")
          .required()
          .action((x,c) => c.copy(table = x))
          .text("table to write in BQ")
        opt[String]('p', "parition")
          .required()
          .action((x,c) => c.copy(parition = x))
          .text("parition table")
        opt[String]('d', "descPartition")
          .action((x,c) => c.copy(descPartition = x))
          .text("description of partition table")
        help("help").text("print this usage text")
      }

      parser.parse(args, Config()) map { config =>
        schema = config.schema
        table = config.table
        parition = config.parition
        descPartition = config.descPartition

        println("[END] Scopt Parser Arguments")

        val arg: String = "Arguments used are: \n- " +
          "Schema: " + schema + "\n- " +
          "Table: " + table + "\n- " +
          "Partition: " + parition + "\n- " +
          "Description Partition: " + descPartition
        //showing args
        println(arg)

        println("[START] Process Ingestion Job")
        //val ingestion = new Ingestion(formatInput, useCase, inputLocation, delimiter, partition, targetLocation, key, infoEncrypt, tblsecurity)
        //ingestion.initProcess()
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
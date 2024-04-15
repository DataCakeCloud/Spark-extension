/*
 * Copyright (c) 2018 Shareit.com Co., Ltd. All Rights Reserved.
 */
package com.ushareit.sql

import java.io.{ByteArrayOutputStream, File}
import java.util
import java.util.{Base64, ServiceLoader}
import java.util.regex.Pattern
import java.util.zip.Inflater

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import org.slf4j.LoggerFactory

import scala.io.Source

object SparkSubmitSql {
  private lazy val log = LoggerFactory.getLogger(this.getClass)

  var spark: SparkSession = _

  private def isBase64(str: String): Boolean = {
    val base64Pattern =
      "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$"
    Pattern.matches(base64Pattern, str)
  }


  private def decode(str: String): String = {
    val dataBytes = Base64.getDecoder.decode(str.getBytes())
    val decompresser = new Inflater()
    decompresser.reset()
    decompresser.setInput(dataBytes)
    val outputStream = new ByteArrayOutputStream(dataBytes.length)
    val buf = new Array[Byte](1024)
    while (!decompresser.finished()) {
      val i = decompresser.inflate(buf)
      outputStream.write(buf, 0, i)
    }
    outputStream.close()
    decompresser.end()
    outputStream.toString()
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      log.error("Usage: [-e string] || [-f string] ")
      sys.exit(1)
    }
    var sql: String = null
    var file: String = null
    var output: String = null
    var format: String = "csv"
    var countFlag: Boolean = false
    var sep: String = null
    var header: String = null
    var columnType: String = null
    args.sliding(2, 2).toList.collect {
      case Array("-e", argSql: String) =>
        val i = argSql.length / 1024
        val count = argSql.trim.count(_ == ' ')
        log.info(s"i = $i")
        log.info(s"count = $count")
        val tmp = if (count <= i + 1) {
          argSql.replaceAll(" ", "")
        } else {
          argSql
        }
        log.info(s"tmp = $tmp")
        if (isBase64(tmp)) {
          log.info("sql = decode(tmp)")
          sql = decode(tmp)
        } else {
          log.info("sql = argSql")
          sql = argSql
        }
      case Array("-f", argFile: String) => file = argFile
      case Array("-o", argOutput: String) => output = argOutput
      case Array("-format", argFormat: String) => format = argFormat
      case Array("-count", argCount: String) => countFlag = true
      case Array("-sep", argSep: String) => sep = argSep
      case Array("-header", argHeader: String) => header = argHeader
      case Array("-colType", argType: String) => columnType = argType
    }
    log.info("sql:" + sql)
    log.info("file:" + file)
    log.info("output:" + output)
    log.info("format:" + format)
    log.info("countFlag:" + countFlag)
    log.info("sep:" + sep)
    log.info("header:" + header)
    log.info("colType:" + columnType)

    assert(StringUtils.isNotBlank(sql) || StringUtils.isNotBlank(file), "should set -e || -f")

    if (StringUtils.isBlank(sep)) {
      sep = "\u0001"
    }

    if (StringUtils.isBlank(header)) {
      header = "true"
    }

    spark = initSpark()

    if (sql != null) {
      val arr = sql.split(";")
      processCmd(spark, arr, output, format, countFlag, sep, header, columnType)
    } else {
      val arr = parse(file).split(";")
      processCmd(spark, arr, output, format, countFlag, sep, header, columnType)
    }
    spark.stop()
  }

  private def processCmd(spark: SparkSession, arr: Array[String],
                         output: String, format: String, countFlag: Boolean,
                         sep: String, header: String, colType: String) = {
    if (StringUtils.isNotBlank(output) && StringUtils.isNotBlank(format)
    ) {
      var df: DataFrame = null
      for (i <- 1 to arr.length) {
        val oneCmd = arr(i - 1)
        if (StringUtils.isNotBlank(oneCmd)) {
          df = spark.sql(oneCmd)
        }
        log.info(s"i = $i , arr.length = ${arr.length}")
        if (i == arr.length) {
          if (oneCmd.trim.toLowerCase.startsWith("select") ||
            oneCmd.trim.toLowerCase.startsWith("show") ||
            oneCmd.trim.toLowerCase.startsWith("with")) {
            log.info("write to file")
            if (countFlag) {
              val root = new Path(output)
              val fs = root.getFileSystem(spark.sessionState.newHadoopConf())
              val outputFile = fs.create(new Path(root, ".COUNT"))
              try {
                outputFile.write(df.count().toString.getBytes("UTF-8"))
                outputFile.close()
              } finally {
                outputFile.close()
              }
            }
            var dfwrite = df
              .write
              .format(format)
              .mode(SaveMode.Append)
            if ("parquet".equals(format)) {
              dfwrite = dfwrite
                .option("compression", "gzip")
            } else if ("csv".equals(format) ) {
              dfwrite = dfwrite
                .option("sep", sep)
                .option("header", header)
                .option("emptyValue", "")
            } else if ("text".equals(format)) {
              /*dfwrite
                .format("csv")
                .option("sep", sep)
                .option("header", header)
                .option("emptyValue", "")
                .save(output)*/
              df.persist()
              df.take(1)

              var dfrdd = df.rdd.map(_.mkString(sep))
              if ("true".equals(header)) {
                if ("true".equals(colType)) {
                  val colTypeDf = spark.sparkContext.parallelize(
                    Array(df.schema.fields.map(field => field.dataType.typeName).mkString(sep)), 1)
                  dfrdd = colTypeDf.union(dfrdd)
                }
                val headers = spark.sparkContext.parallelize(Array(df.columns.mkString(sep)), 1)
                dfrdd = headers.union(dfrdd)
              }
              dfrdd.saveAsTextFile(output)
            }
            if (!"text".equals(format)) {
              dfwrite.save(output)
            }
          } else {
            df.rdd.saveAsTextFile(output)
          }
        }
      }

    } else {
      for (i <- 1 to arr.length) {
        val oneCmd = arr(i - 1)
        if (StringUtils.isNotBlank(oneCmd)) {
          spark.sql(oneCmd).take(1)
        }
      }
    }
  }

  /**
   * init spark
   *
   * @param local
   * @return
   */

  def initSpark(local: Boolean = false): SparkSession = {
    val conf = new SparkConf()
    if (spark == null) {
      if (local) {
        conf.setMaster("local[4]")
          .set("spark.executor.memory", "50m")
          .set("spark.driver.memory", "50m")
          .set("spark.testing", "")
        spark = SparkSession.builder()
          .config(conf)
          .getOrCreate()

      } else {
        spark = SparkSession.builder()
          .enableHiveSupport()
          .config(conf)
          .getOrCreate()
      }
    }
    // scalastyle:off runtimeaddshutdownhook
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        // scalastyle:off println
        println("do some thing before shut down")
        // scalastyle:on println
      }
    })
    // scalastyle:on runtimeaddshutdownhook
    spark
  }


  def parse(file: String): String = {
    // parse file
    val source = Source.fromFile(file, "UTF-8")
    val contents = source.mkString
    source.close()
    contents
  }

}

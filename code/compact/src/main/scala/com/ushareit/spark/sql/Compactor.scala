package com.ushareit.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

trait Compactor extends Serializable with Logging {
  def compact(conf: Configuration, compactFrom: Array[String], compactTo: String)
}

object Compactor extends Serializable {

  def getCompactor(fileFormat: String): Compactor = {
    fileFormat match {
      case "org.apache.parquet.hadoop.ParquetOutputFormat" =>
        new ParquetCompactor()
      case "org.apache.orc.mapreduce.OrcOutputFormat" | "org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat" =>
        new OrcCompactor()
      case "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat" =>
        new TextCompactor()
      case _ =>
        throw new IllegalArgumentException("Not supported file format:" + fileFormat)
    }

  }
}

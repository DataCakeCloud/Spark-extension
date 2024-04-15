package com.ushareit.parser

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

class AddDefaultDbNameExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions
      .injectParser((sparkSession: SparkSession, _: ParserInterface) =>
        new AddDefaultDbNameSqlParser(sparkSession))
  }

}

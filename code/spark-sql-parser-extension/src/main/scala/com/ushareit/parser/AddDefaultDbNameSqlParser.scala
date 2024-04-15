package com.ushareit.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.SqlBaseParser
import org.apache.spark.sql.execution.SparkSqlParser

class AddDefaultDbNameSqlParser(sparkSession: SparkSession) extends SparkSqlParser {

  override protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    val dbName = sparkSession.conf.getOption("spark.sql.default.dbName")
    if (dbName.isDefined) {
      sparkSession.sessionState.catalogManager.setCurrentNamespace(Array(dbName.get))
      log.info(s"current catalog Name: ${sparkSession.sessionState.catalogManager.currentCatalog.name()}, " +
        s"current namespace: ${sparkSession.sessionState.catalogManager.currentNamespace.mkString(".")}")
    }
    super.parse(command)(toResult)
  }
}

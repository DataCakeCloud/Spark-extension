package com.ushareit.spark.sql
import java.util

import com.google.common.base.Predicates
import com.google.common.collect.Collections2
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.{OrcFile, StripeInformation}
import scala.util.control.Breaks._

class OrcCompactor extends Compactor {
  override def compact(conf: Configuration, compactFrom: Array[String], compactTo: String): Unit = {
    val writeOptions = OrcFile.writerOptions(conf)
    //val compactFromPaths = util.Arrays.asList(compactFrom.map(new Path(_)): _*)
    val compactFromPaths = util.Arrays.asList(optimizeCompactFrom(compactFrom, conf).map(new Path(_)): _*)
    val successCompactFromPaths = OrcFile.mergeFiles(new Path(compactTo), writeOptions, compactFromPaths)
    if (successCompactFromPaths.size() != compactFrom.length) {
      val failedCompactFromPaths = Collections2.filter(compactFromPaths, Predicates.not(Predicates.in(successCompactFromPaths)))
      val allFilesEmpty = isAllFilesEmpty(conf, failedCompactFromPaths)
      if (!allFilesEmpty) {
        throw new IllegalStateException("Merge ORC failed due to files cannot be merged:" + failedCompactFromPaths)
      }
    }
  }
  private def optimizeCompactFrom(compactFromPaths: Array[String], conf: Configuration): Array[String] = {
    val readOptions = OrcFile.readerOptions(conf)
    var index = -1
    breakable {
      for (compactFromPath <- compactFromPaths) {
        index += 1
        val reader = OrcFile.createReader(new Path(compactFromPath), readOptions)
        val numberOfRows = reader.getStripes.stream().mapToLong(_.getNumberOfRows).sum()
        if (numberOfRows > 0) {
          break()
        }
      }
    }

    if (index > -1 && index != 0) {
      val tmp = compactFromPaths(0)
      compactFromPaths(0) = compactFromPaths(index)
      compactFromPaths(index) = tmp
    }
    compactFromPaths
  }

  private def isAllFilesEmpty(conf: Configuration, failedCompactFromPaths: util.Collection[Path]): Boolean = {
    val readOptions = OrcFile.readerOptions(conf)
    failedCompactFromPaths.forEach(failedCompactFromPath => {
      val reader = OrcFile.createReader(failedCompactFromPath, readOptions)
      val numberOfRows = reader.getStripes.toArray[StripeInformation](Array()).map(_.getNumberOfRows).sum
      if (numberOfRows != 0) {
        return false
      }
    })
    true
  }
}

package com.ushareit.spark.sql
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.sql.execution.datasources.CodecStreams

import scala.util.control.Breaks._

class TextCompactor extends Compactor {
  override def compact(configuration: Configuration, compactFrom: Array[String], compactTo: String): Unit = {
    val destPath = new Path(compactTo)
    val outputStream = CodecStreams.createOutputStream(new TaskAttemptContextImpl(configuration, new TaskAttemptID()), destPath)
    compactFrom.foreach(filePath => {
      val inputFilePath = new Path(filePath)
      val fs = inputFilePath.getFileSystem(configuration)
      val inputFileLen = fs.getFileStatus(inputFilePath).getLen
      val bytes = new Array[Byte](inputFileLen.toInt)
      var offset = 0
      val inputStream = CodecStreams.createInputStream(configuration, inputFilePath)
      logDebug(s"start read $filePath")
      breakable {
        while (offset < bytes.length) {
          var i = inputStream.read(bytes, offset, bytes.length - offset)
          logDebug(s"read length: $i")
          if (i <= 0) {
            break()
          }
          offset += i
        }
      }
      inputStream.close()
      logDebug(s"close file: $filePath")
      outputStream.write(bytes, 0, bytes.length)
    })
    outputStream.close()
  }
}

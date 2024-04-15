package com.ushareit.spark.sql

import java.io.FileNotFoundException

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.ipc.RemoteException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class CompactFilesCommitProtocol(jobId: String,
                                 path: String,
                                 dynamicPartitionOverwrite: Boolean = false)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {

  import FileCommitProtocol._

  @transient private var fileOutputCommitter: FileOutputCommitter = _
  private var taskTempFile: String = _
  private var enableCompact = true
  private val compactStagingPath = new Path(path, s".compactStaging-$jobId")
  private val COMPACT_SIZE_CONF = "spark.compact.size"
  private val COMPACT_SMALLFILE_SIZE_CONF = "spark.compact.smallfile.size"
  private val COMPACT_ENABLED_CONF = "compact.enabled"

  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    val trace = Thread.currentThread.getStackTrace
    logInfo(s"invoke method: ${trace(1).getMethodName}")
    val outputCommitter = super.setupCommitter(context)
    outputCommitter match {
      case committer: FileOutputCommitter =>
        fileOutputCommitter = committer
      case _ =>
        enableCompact = false
        logError("Cannot compact small files of " + outputCommitter.getClass)
    }
    outputCommitter;
  }

  override def setupJob(jobContext: JobContext): Unit = {
    super.setupJob(jobContext)
    enableCompact = jobContext.getConfiguration.get(COMPACT_ENABLED_CONF, "true").toBoolean
    if (enableCompact) {
      val fs = compactStagingPath.getFileSystem(jobContext.getConfiguration)
      fs.mkdirs(compactStagingPath)
    }

  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    super.setupTask(taskContext)
    enableCompact = taskContext.getConfiguration.get(COMPACT_ENABLED_CONF, "true").toBoolean
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    val trace = Thread.currentThread.getStackTrace
    logInfo(s"invoke method: ${trace(1).getMethodName}")
    logInfo(s"commitJob path: $path")
    if (enableCompact) {
      compactFiles(jobContext)
      super.commitJob(jobContext, taskCommits)
      new Path(path).getFileSystem(jobContext.getConfiguration).delete(compactStagingPath, true)
    } else {
      super.commitJob(jobContext, taskCommits)
    }

  }

  def fillFilePaths(sparkContext: SparkContext, fs: FileSystem, fileStatuses: Array[FileStatus], filePaths: mutable.HashMap[String, ArrayBuffer[String]], compactSize: Long, smallFileSize: Long): Unit = {
    var tmpLen = 0L
    var index = 0
/*    val rdd = sparkContext.parallelize(fileStatuses)
    sparkContext.runJob(rdd, (taskContext: TaskContext, iter: Iterator[FileStatus]) => {

      1
    }, rdd.partitions.indices, (index, res: Int) => {})*/
    for (status <- fileStatuses) {
      logDebug(s"filePath: ${status.getPath}")
      if (status.isDirectory) {
        fillFilePaths(sparkContext, fs, fs.listStatus(status.getPath), filePaths, compactSize, smallFileSize)
      } else {
        val filePath = status.getPath
        val fileNameToCompact = filePath.getName
        val uri = compactStagingPath.toUri.relativize(filePath.toUri)
        val outPutDir = new Path(path, uri.getPath).getParent
        val fileToCompact = new Path(outPutDir, fileNameToCompact)
        try {
          val len = fs.getFileStatus(fileToCompact).getLen
          if (len < smallFileSize) {
            tmpLen += len
            if (tmpLen > compactSize) {
              index += 1
              tmpLen = 0
            }
            val compactName = f"compact-$index%05d" + fileNameToCompact.substring(10)
            val fileName = new Path(outPutDir, compactName).toString
            val filesToCompact = filePaths.getOrElseUpdate(fileName, new ArrayBuffer[String]())
            filesToCompact.append(fileToCompact.toString)
            logDebug(s"$fileToCompact will compact to $fileName")
          }
        } catch {
          case e: FileNotFoundException =>
            logWarning(s"$fileToCompact maybe not be committed")
          case e: Exception =>
            throw e;
        }
      }
    }
  }

  def compactFiles(jobContext: JobContext): Unit = {
    val fs = compactStagingPath.getFileSystem(jobContext.getConfiguration)
    val fileStatus = fs.listStatus(compactStagingPath)
    val filePaths = new mutable.HashMap[String, ArrayBuffer[String]]()
    val sparkContext = SparkSession.getActiveSession.get.sparkContext
    val sparkConf = sparkContext.getConf
    val compactSize = sparkConf.get(COMPACT_SIZE_CONF, "134217728").toLong
    val smallFileSize = sparkConf.get(COMPACT_SMALLFILE_SIZE_CONF, "67108864").toLong
    logInfo("start fill file paths")
    fillFilePaths(sparkContext, fs, fileStatus, filePaths, compactSize, smallFileSize)
    logInfo("finish fill file paths")
    if (filePaths.isEmpty) {
      return
    } else if (filePaths.size == 1 && filePaths.head._2.length <= 1) {
      return
    }
    val rdd = sparkContext.parallelize(filePaths.toSeq, filePaths.size)
    val configuration = new SerializableConfiguration(jobContext.getConfiguration)
    val outPutFormatClass = jobContext.getOutputFormatClass.getName
    sparkContext
      .runJob(rdd, (taskContext: TaskContext, iter: Iterator[(String, ArrayBuffer[String])]) => {
        while (iter.hasNext) {
          val fileToCompact = iter.next()
          val compactor = Compactor.getCompactor(outPutFormatClass)
          val fileSystem = new Path(path).getFileSystem(configuration.value)
          try {
            compactor.compact(configuration.value, fileToCompact._2.toArray, fileToCompact._1)
            fileToCompact._2.foreach(file => fileSystem.delete(new Path(file), false))
          } catch {
            case re: RemoteException => {
              if (fileSystem.exists(new Path(fileToCompact._1))) {
                if (re.getClassName.equals("java.io.FileNotFoundException")) {
                  logWarning("Compact failed, should be another attempt has succeed, delete temp file, message:" + re.toString)
                  //won't throw exception so can continue delete source files
                } else {
                  fileSystem.delete(new Path(fileToCompact._1), false)
                  throw re
                }
              } else {
                throw re
              }
            }
            case fnfe: FileNotFoundException => {
              if (fileSystem.exists(new Path(fileToCompact._1))) {
                logWarning("Compact failed, should be another attempt has succeed, delete temp file, message:" + fnfe.toString)
                //won't throw exception so can continue delete source files
              } else {
                throw fnfe
              }
            }
            case e: Exception => {
              logError("Compact failed. ", e)
              fileSystem.delete(new Path(fileToCompact._1), false)
              throw e
            }
          }
        }
      })
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    logDebug(s"fileOutputCommitterfile class name: ${fileOutputCommitter.getClass.getCanonicalName}")
    logDebug(s"needsTaskCommit: ${fileOutputCommitter.needsTaskCommit(taskContext)}")
    super.commitTask(taskContext)
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    taskTempFile = super.newTaskTempFile(taskContext, dir, ext)
    if (enableCompact) {
      val trace = Thread.currentThread.getStackTrace
      logDebug(s"invoke method: ${trace(1).getMethodName}")
      val taskTempFilePath = new Path(taskTempFile)
      val fs = taskTempFilePath.getFileSystem(taskContext.getConfiguration)
      val stagingDir: Path = fileOutputCommitter match {
        case _ if dynamicPartitionOverwrite =>
          assert(dir.isDefined,
            "The dataset to be written must be partitioned when dynamicPartitionOverwrite is true.")
          new Path(path, ".spark-staging-" + jobId)
        // For FileOutputCommitter it has its own staging path called "work path".
        case f: FileOutputCommitter =>
          new Path(Option(f.getWorkPath).map(_.toString).getOrElse(path))
        case _ => new Path(path)
      }
      val uri = stagingDir.toUri.relativize(taskTempFilePath.toUri)
      val compactStagingFile = new Path(compactStagingPath, new Path(uri))
      logDebug(s"create compact staging file: $compactStagingFile")
      fs.create(compactStagingFile).close()
    }
    taskTempFile
  }

  override def abortJob(jobContext: JobContext): Unit = {
    super.abortJob(jobContext)
    if (enableCompact) {
      val fs = compactStagingPath.getFileSystem(jobContext.getConfiguration)
      fs.delete(compactStagingPath, true)
    }
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    super.abortTask(taskContext)
    if (enableCompact) {
      val taskAttemptPath = fileOutputCommitter.getWorkPath
      val taskTempFilePath = new Path(taskTempFile)
      val fs = taskAttemptPath.getFileSystem(taskContext.getConfiguration)
      val uri = taskAttemptPath.toUri.relativize(taskTempFilePath.toUri)
      val compactStagingFile = new Path(compactStagingPath, new Path(uri))
      fs.delete(compactStagingFile, true)
    }
  }
}


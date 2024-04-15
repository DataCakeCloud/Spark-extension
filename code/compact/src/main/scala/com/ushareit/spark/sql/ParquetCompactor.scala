package com.ushareit.spark.sql
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData, GlobalMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetWriter}
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.apache.parquet.schema.MessageType

class ParquetCompactor extends Compactor {
  override def compact(conf: Configuration, compactFrom: Array[String], compactTo: String): Unit = {
    val compactFromPaths = compactFrom.map(new Path(_))
    val mergedMeta = mergeMetadataFiles(compactFromPaths, conf).getFileMetaData

    val writer = new ParquetFileWriter(HadoopOutputFile.fromPath(new Path(compactTo), conf),
      mergedMeta.getSchema, ParquetFileWriter.Mode.CREATE,
      ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
    writer.start()
    logInfo(s"start writer to $compactTo")
    for (input <- compactFromPaths) {
      writer.appendFile(HadoopInputFile.fromPath(input, conf))
    }
    writer.end(mergedMeta.getKeyValueMetaData)
    logInfo(s"end writer to $compactTo")
  }

  private def mergeMetadataFiles(files: Array[Path], conf: Configuration): ParquetMetadata = {
    if (files.isEmpty) {
      throw new IllegalArgumentException("Cannot merge an empty list of metadata")
    }
    var globalMetaData: GlobalMetaData = null
    val blocks = new util.ArrayList[BlockMetaData]
    logInfo("merge metadata files")
    for (p <- files) {
      val file = HadoopInputFile.fromPath(p, conf)
      val options = HadoopReadOptions.builder(conf).withMetadataFilter(ParquetMetadataConverter.NO_FILTER).build
      val reader = new ParquetFileReader(file, options)
      val pmd = reader.getFooter
      val fmd = reader.getFileMetaData
      logInfo(s"merge file $p into global metadata")
      globalMetaData = mergeInto(fmd, globalMetaData, true)
      blocks.addAll(pmd.getBlocks)
      reader.close()
    }
    // collapse GlobalMetaData into a single FileMetaData, which will throw if they are not compatible
    new ParquetMetadata(globalMetaData.merge, blocks)
  }

  private def mergeInto(toMerge: FileMetaData, mergedMetadata: GlobalMetaData, strict: Boolean): GlobalMetaData = {
    var schema: MessageType = null
    val newKeyValues = new util.HashMap[String, util.Set[String]]()
    val createdBy = new util.HashSet[String]()
    if (mergedMetadata != null) {
      schema = mergedMetadata.getSchema
      newKeyValues.putAll(mergedMetadata.getKeyValueMetaData)
      createdBy.addAll(mergedMetadata.getCreatedBy)
    }
    if ((schema == null && toMerge.getSchema != null) || (schema != null && !schema.equals(toMerge.getSchema))) {
      schema = mergeInto(toMerge.getSchema, schema, strict)
    }
    toMerge.getKeyValueMetaData.entrySet.forEach(entry => {
      var values = newKeyValues.get(entry.getKey)
      if (values == null) {
        values = new util.LinkedHashSet[String]()
        newKeyValues.put(entry.getKey, values)
      }
      values.add(entry.getValue)
    })
    createdBy.add(toMerge.getCreatedBy)
    new GlobalMetaData(schema, newKeyValues, createdBy)
  }

  private def mergeInto(toMerge: MessageType, mergedSchema: MessageType, strict: Boolean): MessageType = {
    if (mergedSchema == null) {
      return toMerge
    }
    mergedSchema.union(toMerge, strict)
  }

}

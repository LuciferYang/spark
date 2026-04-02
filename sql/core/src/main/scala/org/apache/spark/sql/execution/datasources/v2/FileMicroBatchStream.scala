/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, ReadAllAvailable, ReadLimit, ReadMaxBytes, ReadMaxFiles, SupportsAdmissionControl, SupportsTriggerAvailableNow}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.streaming.runtime.{FileStreamOptions, FileStreamSource, FileStreamSourceLog, FileStreamSourceOffset, MetadataLogFileIndex, SerializedOffset}
import org.apache.spark.sql.execution.streaming.runtime.FileStreamSource.FileEntry
import org.apache.spark.sql.execution.streaming.sinks.FileStreamSink
import org.apache.spark.sql.types.StructType

/**
 * A [[MicroBatchStream]] implementation for file-based streaming reads using the V2 data source
 * API. This is the V2 counterpart of the V1 [[FileStreamSource]].
 *
 * It reuses the same infrastructure as [[FileStreamSource]]:
 * - [[FileStreamSourceLog]] for metadata tracking
 * - [[FileStreamSourceOffset]] for offset representation
 * - [[FileStreamSource.SeenFilesMap]] for deduplication
 * - [[FileStreamOptions]] for parsed streaming options
 */
class FileMicroBatchStream(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    fileScan: FileScan,
    path: String,
    fileFormatClassName: String,
    schema: StructType,
    partitionColumns: Seq[String],
    metadataPath: String,
    options: Map[String, String])
  extends MicroBatchStream
  with SupportsAdmissionControl
  with SupportsTriggerAvailableNow
  with Logging {

  import FileMicroBatchStream._

  private val sourceOptions = new FileStreamOptions(options)

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  @transient private val fs = new Path(path).getFileSystem(hadoopConf)

  private val qualifiedBasePath: Path = {
    fs.makeQualified(new Path(path)) // can contain glob patterns
  }

  private val sourceCleaner: Option[FileStreamSource.FileStreamSourceCleaner] =
    FileStreamSource.FileStreamSourceCleaner(fs, qualifiedBasePath, sourceOptions, hadoopConf)

  private val metadataLog =
    new FileStreamSourceLog(FileStreamSourceLog.VERSION, sparkSession, metadataPath)
  private var metadataLogCurrentOffset = metadataLog.getLatest().map(_._1).getOrElse(-1L)

  /** Maximum number of new files to be considered in each batch */
  private val maxFilesPerBatch = sourceOptions.maxFilesPerTrigger

  /** Maximum number of new bytes to be considered in each batch */
  private val maxBytesPerBatch = sourceOptions.maxBytesPerTrigger

  private val fileSortOrder = if (sourceOptions.latestFirst) {
    logWarning(
      """'latestFirst' is true. New files will be processed first, which may affect the watermark
        |value. In addition, 'maxFileAge' will be ignored.""".stripMargin)
    implicitly[Ordering[Long]].reverse
  } else {
    implicitly[Ordering[Long]]
  }

  private val maxFileAgeMs: Long = if (sourceOptions.latestFirst &&
    (maxFilesPerBatch.isDefined || maxBytesPerBatch.isDefined)) {
    Long.MaxValue
  } else {
    sourceOptions.maxFileAgeMs
  }

  private val fileNameOnly = sourceOptions.fileNameOnly
  if (fileNameOnly) {
    logWarning("'fileNameOnly' is enabled. Make sure your file names are unique (e.g. using " +
      "UUID), otherwise, files with the same name but under different paths will be considered " +
      "the same and causes data lost.")
  }

  private val maxCachedFiles = sourceOptions.maxCachedFiles

  private val discardCachedInputRatio = sourceOptions.discardCachedInputRatio

  /** A mapping from a file that we have processed to some timestamp it was last modified. */
  val seenFiles = new FileStreamSource.SeenFilesMap(maxFileAgeMs, fileNameOnly)

  private var allFilesForTriggerAvailableNow: Seq[NewFileEntry] = _

  // Restore seenFiles from metadata log
  metadataLog.restore().foreach { entry =>
    seenFiles.add(entry.sparkPath, entry.timestamp)
  }
  seenFiles.purge()

  logInfo(log"maxFilesPerBatch = ${MDC(LogKeys.NUM_FILES, maxFilesPerBatch)}, " +
    log"maxBytesPerBatch = ${MDC(LogKeys.NUM_BYTES, maxBytesPerBatch)}, " +
    log"maxFileAgeMs = ${MDC(LogKeys.TIME_UNITS, maxFileAgeMs)}")

  private var unreadFiles: Seq[NewFileEntry] = _

  /**
   * If the source has a metadata log indicating which files should be read, then we should use it.
   * Only when user gives a non-glob path that will we figure out whether the source has some
   * metadata log.
   *
   * None        means we don't know at the moment
   * Some(true)  means we know for sure the source DOES have metadata
   * Some(false) means we know for sure the source DOES NOT have metadata
   */
  @volatile private[sql] var sourceHasMetadata: Option[Boolean] =
    if (SparkHadoopUtil.get.isGlobPath(new Path(path))) Some(false) else None

  // ---------------------------------------------------------------------------
  // SparkDataStream methods
  // ---------------------------------------------------------------------------

  override def initialOffset(): streaming.Offset = FileStreamSourceOffset(-1L)

  override def deserializeOffset(json: String): streaming.Offset = {
    FileStreamSourceOffset(SerializedOffset(json))
  }

  override def commit(end: streaming.Offset): Unit = {
    val logOffset = FileStreamSourceOffset(
      end.asInstanceOf[org.apache.spark.sql.execution.streaming.Offset]).logOffset

    sourceCleaner.foreach { cleaner =>
      val files = metadataLog.get(Some(logOffset), Some(logOffset)).flatMap(_._2)
      val validFileEntries = files.filter(_.batchId == logOffset)
      logDebug(s"completed file entries: ${validFileEntries.mkString(",")}")
      validFileEntries.foreach(cleaner.clean)
    }
  }

  override def stop(): Unit = {
    sourceCleaner.foreach(_.stop())
  }

  // ---------------------------------------------------------------------------
  // SupportsAdmissionControl methods
  // ---------------------------------------------------------------------------

  override def getDefaultReadLimit: ReadLimit = {
    maxFilesPerBatch.map(ReadLimit.maxFiles).getOrElse(
      maxBytesPerBatch.map(ReadLimit.maxBytes).getOrElse(super.getDefaultReadLimit)
    )
  }

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    Some(fetchMaxOffset(limit)).filterNot(_.logOffset == -1).orNull
  }

  // ---------------------------------------------------------------------------
  // MicroBatchStream methods
  // ---------------------------------------------------------------------------

  override def latestOffset(): streaming.Offset = {
    latestOffset(null, getDefaultReadLimit)
  }

  override def planInputPartitions(
      start: streaming.Offset,
      end: streaming.Offset): Array[InputPartition] = {
    val startOffset = FileStreamSourceOffset(
      start.asInstanceOf[org.apache.spark.sql.execution.streaming.Offset]).logOffset
    val endOffset = FileStreamSourceOffset(
      end.asInstanceOf[org.apache.spark.sql.execution.streaming.Offset]).logOffset

    assert(startOffset <= endOffset)
    val files = metadataLog.get(Some(startOffset + 1), Some(endOffset)).flatMap(_._2)
    logInfo(log"Processing ${MDC(LogKeys.NUM_FILES, files.length)} files from " +
      log"${MDC(LogKeys.FILE_START_OFFSET, startOffset + 1)}:" +
      log"${MDC(LogKeys.FILE_END_OFFSET, endOffset)}")
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))

    // Build an InMemoryFileIndex from the file entries.
    // For non-glob paths, pass basePath so partition discovery uses the streaming
    // source root instead of the individual file's parent directory.
    val filePaths = files.map(_.sparkPath.toPath).toSeq
    val indexOptions = if (!SparkHadoopUtil.get.isGlobPath(new Path(path))) {
      options + ("basePath" -> qualifiedBasePath.toString)
    } else {
      options
    }
    val tempFileIndex = new InMemoryFileIndex(
      sparkSession, filePaths, indexOptions, Some(schema))

    fileScan.withFileIndex(tempFileIndex).planInputPartitions()
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    fileScan.createReaderFactory()
  }

  // ---------------------------------------------------------------------------
  // SupportsTriggerAvailableNow methods
  // ---------------------------------------------------------------------------

  override def prepareForTriggerAvailableNow(): Unit = {
    allFilesForTriggerAvailableNow = fetchAllFiles()
  }

  // ---------------------------------------------------------------------------
  // Internal methods - ported from FileStreamSource
  // ---------------------------------------------------------------------------

  /**
   * Split files into a selected/unselected pair according to a total size threshold.
   * Always puts the 1st element in a left split and keep adding it to a left split
   * until reaches a specified threshold or [[Long.MaxValue]].
   */
  private def takeFilesUntilMax(files: Seq[NewFileEntry], maxSize: Long)
    : (FilesSplit, FilesSplit) = {
    var lSize = BigInt(0)
    var rSize = BigInt(0)
    val lFiles = ArrayBuffer[NewFileEntry]()
    val rFiles = ArrayBuffer[NewFileEntry]()
    files.zipWithIndex.foreach { case (file, i) =>
      val newSize = lSize + file.size
      if (i == 0 || rFiles.isEmpty && newSize <= Long.MaxValue && newSize <= maxSize) {
        lSize += file.size
        lFiles += file
      } else {
        rSize += file.size
        rFiles += file
      }
    }
    (FilesSplit(lFiles.toSeq, lSize), FilesSplit(rFiles.toSeq, rSize))
  }

  /**
   * Returns the maximum offset that can be retrieved from the source.
   *
   * `synchronized` on this method is for solving race conditions in tests. In the normal usage,
   * there is no race here, so the cost of `synchronized` should be rare.
   */
  private def fetchMaxOffset(limit: ReadLimit): FileStreamSourceOffset = synchronized {
    val newFiles = if (unreadFiles != null && unreadFiles.nonEmpty) {
      logDebug(s"Reading from unread files - ${unreadFiles.size} files are available.")
      unreadFiles
    } else {
      // All the new files found - ignore aged files and files that we have seen.
      // Use the pre-fetched list of files when Trigger.AvailableNow is enabled.
      val allFiles = if (allFilesForTriggerAvailableNow != null) {
        allFilesForTriggerAvailableNow
      } else {
        fetchAllFiles()
      }
      allFiles.filter {
        case NewFileEntry(path, _, timestamp) => seenFiles.isNewFile(path, timestamp)
      }
    }

    val shouldCache = !sourceOptions.latestFirst && allFilesForTriggerAvailableNow == null

    // Obey user's setting to limit the number of files in this batch trigger.
    val (batchFiles, unselectedFiles) = limit match {
      case files: ReadMaxFiles if shouldCache =>
        // we can cache and reuse remaining fetched list of files in further batches
        val (bFiles, usFiles) = newFiles.splitAt(files.maxFiles())
        if (usFiles.size < files.maxFiles() * discardCachedInputRatio) {
          // Discard unselected files if the number of files are smaller than threshold.
          logTrace(s"Discarding ${usFiles.length} unread files as it's smaller than threshold.")
          (bFiles, null)
        } else {
          (bFiles, usFiles)
        }

      case files: ReadMaxFiles =>
        // don't use the cache, just take files for the next batch
        (newFiles.take(files.maxFiles()), null)

      case files: ReadMaxBytes if shouldCache =>
        // we can cache and reuse remaining fetched list of files in further batches
        val (FilesSplit(bFiles, _), FilesSplit(usFiles, rSize)) =
          takeFilesUntilMax(newFiles, files.maxBytes())
        if (rSize.toDouble < (files.maxBytes() * discardCachedInputRatio)) {
          logTrace(s"Discarding ${usFiles.length} unread files as it's smaller than threshold.")
          (bFiles, null)
        } else {
          (bFiles, usFiles)
        }

      case files: ReadMaxBytes =>
        // don't use the cache, just take files for the next batch
        val (FilesSplit(bFiles, _), _) = takeFilesUntilMax(newFiles, files.maxBytes())
        (bFiles, null)

      case _: ReadAllAvailable => (newFiles, null)
    }

    // need to ensure that if maxCachedFiles is set to 0 that the next batch will be forced to
    // list files again
    if (unselectedFiles != null && unselectedFiles.nonEmpty && maxCachedFiles > 0) {
      logTrace(s"Taking first $maxCachedFiles unread files.")
      unreadFiles = unselectedFiles.take(maxCachedFiles)
      logTrace(s"${unreadFiles.size} unread files are available for further batches.")
    } else {
      unreadFiles = null
      logTrace(s"No unread file is available for further batches or maxCachedFiles has been set " +
        s" to 0 to disable caching.")
    }

    batchFiles.foreach { case NewFileEntry(p, _, timestamp) =>
      seenFiles.add(p, timestamp)
      logDebug(s"New file: $p")
    }
    val numPurged = seenFiles.purge()

    logTrace(
      s"""
         |Number of new files = ${newFiles.size}
         |Number of files selected for batch = ${batchFiles.size}
         |Number of unread files = ${Option(unreadFiles).map(_.size).getOrElse(0)}
         |Number of seen files = ${seenFiles.size}
         |Number of files purged from tracking map = $numPurged
       """.stripMargin)

    if (batchFiles.nonEmpty) {
      metadataLogCurrentOffset += 1

      val fileEntries = batchFiles.map { case NewFileEntry(p, _, timestamp) =>
        FileEntry(path = p.urlEncoded, timestamp = timestamp, batchId = metadataLogCurrentOffset)
      }.toArray
      if (metadataLog.add(metadataLogCurrentOffset, fileEntries)) {
        logInfo(log"Log offset set to ${MDC(LogKeys.LOG_OFFSET, metadataLogCurrentOffset)} " +
          log"with ${MDC(LogKeys.NUM_FILES, batchFiles.size)} new files")
      } else {
        throw new IllegalStateException("Concurrent update to the log. Multiple streaming jobs " +
          s"detected for $metadataLogCurrentOffset")
      }
    }

    FileStreamSourceOffset(metadataLogCurrentOffset)
  }

  private def allFilesUsingInMemoryFileIndex(): Seq[FileStatus] = {
    val globbedPaths = SparkHadoopUtil.get.globPathIfNecessary(fs, qualifiedBasePath)
    val fileIdx = new InMemoryFileIndex(
      sparkSession, globbedPaths, options, Some(new StructType))
    fileIdx.allFiles()
  }

  private def allFilesUsingMetadataLogFileIndex(): Seq[FileStatus] = {
    // Note if `sourceHasMetadata` holds, then `qualifiedBasePath` is guaranteed to be a
    // non-glob path
    new MetadataLogFileIndex(sparkSession, qualifiedBasePath,
      CaseInsensitiveMap(options), None).allFiles()
  }

  private def setSourceHasMetadata(newValue: Option[Boolean]): Unit = newValue match {
    case Some(true) =>
      if (sourceCleaner.isDefined) {
        throw QueryExecutionErrors.cleanUpSourceFilesUnsupportedError()
      }
      sourceHasMetadata = Some(true)
    case _ =>
      sourceHasMetadata = newValue
  }

  /**
   * Returns a list of files found, sorted by their timestamp.
   */
  private def fetchAllFiles(): Seq[NewFileEntry] = {
    val startTime = System.nanoTime

    var allFiles: Seq[FileStatus] = null
    sourceHasMetadata match {
      case None =>
        if (FileStreamSink.hasMetadata(
            Seq(path), hadoopConf, sparkSession.sessionState.conf)) {
          setSourceHasMetadata(Some(true))
          allFiles = allFilesUsingMetadataLogFileIndex()
        } else {
          allFiles = allFilesUsingInMemoryFileIndex()
          if (allFiles.isEmpty) {
            // we still cannot decide
          } else {
            // decide what to use for future rounds
            // double check whether source has metadata, preventing the extreme corner case that
            // metadata log and data files are only generated after the previous
            // `FileStreamSink.hasMetadata` check
            if (FileStreamSink.hasMetadata(
                Seq(path), hadoopConf, sparkSession.sessionState.conf)) {
              setSourceHasMetadata(Some(true))
              allFiles = allFilesUsingMetadataLogFileIndex()
            } else {
              setSourceHasMetadata(Some(false))
              // `allFiles` have already been fetched using InMemoryFileIndex in this round
            }
          }
        }
      case Some(true) => allFiles = allFilesUsingMetadataLogFileIndex()
      case Some(false) => allFiles = allFilesUsingInMemoryFileIndex()
    }

    val files = allFiles.sortBy(_.getModificationTime)(fileSortOrder).map { status =>
      NewFileEntry(SparkPath.fromFileStatus(status), status.getLen, status.getModificationTime)
    }
    val endTime = System.nanoTime
    val listingTimeMs = NANOSECONDS.toMillis(endTime - startTime)
    if (listingTimeMs > 2000) {
      // Output a warning when listing files uses more than 2 seconds.
      logWarning(log"Listed ${MDC(LogKeys.NUM_FILES, files.size)} file(s) in " +
        log"${MDC(LogKeys.ELAPSED_TIME, listingTimeMs)} ms")
    } else {
      logTrace(s"Listed ${files.size} file(s) in $listingTimeMs ms")
    }
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))
    files
  }

  override def toString: String = s"FileMicroBatchStream[$qualifiedBasePath]"
}

private[v2] object FileMicroBatchStream {
  /** Newly fetched files metadata holder. */
  private[v2] case class NewFileEntry(path: SparkPath, size: Long, timestamp: Long)

  private case class FilesSplit(files: Seq[NewFileEntry], size: BigInt)
}

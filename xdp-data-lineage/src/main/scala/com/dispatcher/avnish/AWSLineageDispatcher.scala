/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dispatcher.avnish

import com.dispatcher.avnish.AWSLineageDispatcher.{BufferSizeKey, DefaultBufferSize, DefaultFilePermission, FilePermissionsKey, FolderNameKey, ProgramName, pathStringToFsWithPath}
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import za.co.absa.commons.annotation.Experimental
import za.co.absa.commons.config.ConfigurationImplicits.{ConfigurationOptionalWrapper, ConfigurationRequiredWrapper}
import za.co.absa.commons.lang.ARM.using
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe
import za.co.absa.spline.producer.model.v1_1.{ExecutionEvent, ExecutionPlan}
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.commons.s3.SimpleS3Location.SimpleS3LocationExt
import org.apache.hadoop.fs.FSDataInputStream

import java.io.File
import java.io.{BufferedInputStream, FileInputStream}
import java.net.URI
import scala.concurrent.blocking
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ListBuffer

/**
 * A port of https://github.com/AbsaOSS/spline/tree/release/0.3.9/persistence/hdfs/src/main/scala/za/co/absa/spline/persistence/hdfs
 *
 * Note:
 * This class is unstable, experimental, is mostly used for debugging, with no guarantee to work properly
 * for every generic use case in a real production application.
 *
 * It is NOT thread-safe, strictly synchronous assuming a predefined order of method calls: `send(plan)` and then `send(event)`
 */
@Experimental
class AWSLineageDispatcher(folderName: String, permission: FsPermission, bufferSize: Int, programName: String)
  extends LineageDispatcher
    with Logging {

  def this(conf: Configuration) = this(
    folderName = conf.getRequiredString(FolderNameKey) ,
    permission = new FsPermission(conf.getOptionalString(FilePermissionsKey).getOrElse(DefaultFilePermission.toShort.toString)),
    bufferSize = DefaultBufferSize,
    programName = conf.getOptionalString(ProgramName).getOrElse(SparkContext.getOrCreate().appName)
  )

  @volatile
  private var _lastSeenPlan: ExecutionPlan = _

  def getFileName(folderName: String): String = {

    val today = Calendar.getInstance().getTime()
    val timestampFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val appName = SparkContext.getOrCreate().appName.filterNot(_ == ' ')
    if(folderName.endsWith(File.separator)){
      folderName + appName + "_" + SparkContext.getOrCreate().applicationId + "_" + timestampFormat.format(today) + ".log"
    } else {
      folderName + File.pathSeparator + appName + "_"  + SparkContext.getOrCreate().applicationId + "_" + timestampFormat.format(today) + ".log"
    }

  }
  override def send(plan: ExecutionPlan): Unit = {

    val today = Calendar.getInstance().getTime()
    val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val runDateFormat = new SimpleDateFormat("yyyyMMdd")

    val planWithJobName = Map(
      "lineage" -> plan,
      "application" -> programName,
      "applicationId" -> SparkContext.getOrCreate().applicationId,
      "timestamp" ->  timestampFormat.format(today),
      "runDate" -> runDateFormat.format(today)
    )
    val listOfPlans = List(planWithJobName)

    persistToHadoopFs(planWithJobName, listOfPlans.toJson, getFileName(this.folderName))
  }

  override def send(event: ExecutionEvent): Unit = {

  }

  private def appendLineage(jsonStr:String, plan: Map[String, Any]): String = {
    import HarvesterJsonSerDe.impl._

    val extractedList :List[Map[String, Any]] = jsonStr.fromJson[List[Map[String, Any]]]
    var finalList= new ListBuffer[Map[String, Any]]()
    finalList.appendAll(extractedList)
    finalList += plan
    finalList.toJson
  }


  private def persistToHadoopFs(plan: Map[String, Any], content: String, fullLineagePath: String): Unit = blocking {
    val (fs, path) = pathStringToFsWithPath(fullLineagePath)
    logDebug(s"Opening HadoopFs output stream to $path")
    val defaultFilePermission = new FsPermission("777")

    val replication = fs.getDefaultReplication(path)
    val blockSize = fs.getDefaultBlockSize(path)


    if (fs.exists(path) && fs.isFile(path)) {
      val (fsNew, pathNew) = pathStringToFsWithPath(fullLineagePath+"_tmp")
      val inputStream = fs.open(path)
      val outputStream = fsNew.create(pathNew, defaultFilePermission, true, bufferSize, replication, blockSize, null)
      val umask = FsPermission.getUMask(fs.getConf)
      FsPermission.getFileDefault.applyUMask(umask)
      val jsonStr = scala.io.Source.fromInputStream(inputStream).mkString
      val finalStr = appendLineage(jsonStr, plan)
//      IOUtils.copyBytes(inputStream, outputStream, bufferSize)
      logDebug(s"Writing lineage to $pathNew")
      using(outputStream) {
        _.write(finalStr.getBytes("UTF-8"))
      }
      inputStream.close()
      outputStream.close()

      fs.delete(path, true)
      fs.rename(pathNew, path)
      fs.delete(pathNew, true)
    } else {
      val outputStream = fs.create(path, defaultFilePermission, true, bufferSize, replication, blockSize, null)
      val umask = FsPermission.getUMask(fs.getConf)
      FsPermission.getFileDefault.applyUMask(umask)

      logDebug(s"Writing lineage to $path")
      using(outputStream) {
        _.write(content.getBytes("UTF-8"))
      }
    }

  }
}

object AWSLineageDispatcher {
  private val HadoopConfiguration = SparkContext.getOrCreate().hadoopConfiguration

  private val FolderNameKey = "folderName"
  private val FilePermissionsKey = "filePermissions"
  private val BufferSizeKey = "fileBufferSize"
  private val ProgramName = "application"
  private val DefaultBufferSize = 32 * 1024 * 1024
  private val DefaultFilePermission = new FsPermission("777")

  /**
   * Converts string full path to Hadoop FS and Path, e.g.
   * `s3://mybucket1/path/to/file` -> S3 FS + `path/to/file`
   * `/path/on/hdfs/to/file` -> local HDFS + `/path/on/hdfs/to/file`
   *
   * Note, that non-local HDFS paths are not supported in this method, e.g. hdfs://nameservice123:8020/path/on/hdfs/too.
   *
   * @param pathString path to convert to FS and relative path
   * @return FS + relative path
   **/
  def pathStringToFsWithPath(pathString: String): (FileSystem, Path) = {

    pathString.toSimpleS3Location match {
      case Some(s3Location) =>
        val s3Uri = new URI(s3Location.asSimpleS3LocationString) // s3://<bucket>
        val s3Path = new Path(s"/${s3Location.path}") // /<text-file-object-path>

        val fs = FileSystem.get(s3Uri, HadoopConfiguration)
        (fs, s3Path)

      case None => // local hdfs location
        val fs = FileSystem.get(HadoopConfiguration)
        (fs, new Path(pathString))
    }
  }
}



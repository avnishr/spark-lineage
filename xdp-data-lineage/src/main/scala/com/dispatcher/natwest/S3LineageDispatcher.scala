package com.dispatcher.natwest

import com.dispatcher.natwest.S3LineageDispatcher._
import org.apache.commons.configuration.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import za.co.absa.commons.config.ConfigurationImplicits.{ConfigurationOptionalWrapper, ConfigurationRequiredWrapper}
import za.co.absa.commons.lang.ARM.using
import za.co.absa.commons.s3.SimpleS3Location.SimpleS3LocationExt
import za.co.absa.spline.harvester.dispatcher.HDFSLineageDispatcher.pathStringToFsWithPath
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.spline.producer.model.v1_1.{ExecutionEvent, ExecutionPlan}

import java.net.URI
import scala.concurrent.blocking

class S3LineageDispatcher(filename: String, permission: FsPermission, bufferSize: Int)
  extends LineageDispatcher
    with Logging {

  @volatile
  private var _lastSeenPlan: ExecutionPlan = _

  def this(conf: Configuration) = this(
    filename = conf.getRequiredString(FileNameKey),
    permission = new FsPermission(conf.getOptionalString(FilePermissionsKey).getOrElse(DefaultFilePermission.toShort.toString)),
    bufferSize = conf.getOptionalInt(BufferSizeKey).getOrElse(DefaultBufferSize)
  )


  override def send(plan: ExecutionPlan): Unit = {
    this._lastSeenPlan = plan
    this.persistToHadoopFs(plan.toJson, this.filename)
  }

  override def send(event: ExecutionEvent): Unit = {
    // check state
    if (this._lastSeenPlan == null || this._lastSeenPlan.id.get != event.planId)
      throw new IllegalStateException("send(event) must be called strictly after send(plan) method with matching plan ID")

    try {
      val path = s"${this._lastSeenPlan.operations.write.outputSource.stripSuffix("/")}/$filename"
      val planWithEvent = Map(
        "executionPlan" -> this._lastSeenPlan,
        "executionEvent" -> event
      )
      persistToHadoopFs(planWithEvent.toJson, path)
    } finally {
      this._lastSeenPlan = null
    }
  }

  private def persistToHadoopFs(content: String, fullLineagePath: String): Unit = blocking {
    val (fs, path) = pathStringToFsWithPath(fullLineagePath)
    logDebug(s"Opening HadoopFs output stream to $path")

    val replication = fs.getDefaultReplication(path)
    val blockSize = fs.getDefaultBlockSize(path)
    var outputStream = fs.create(path, permission, true, bufferSize, replication, blockSize, null)

    val umask = FsPermission.getUMask(fs.getConf)
    FsPermission.getFileDefault.applyUMask(umask)

    logDebug(s"Writing lineage to $path")
    using(outputStream) {
      _.write(content.getBytes("UTF-8"))
    }
  }
}
object S3LineageDispatcher {

  private val HadoopConfiguration = SparkContext.getOrCreate().hadoopConfiguration

  private val FileNameKey = "fileName"

  private val FilePermissionsKey = "filePermissions"
  private val BufferSizeKey = "fileBufferSize"

  private val DefaultFilePermission = new FsPermission("644")
  private val DefaultBufferSize = 32 * 1024 * 1024

  /**
   * Converts string full path to Hadoop FS and Path, e.g.
   * `s3://mybucket1/path/to/file` -> S3 FS + `path/to/file`
   * `/path/on/hdfs/to/file` -> local HDFS + `/path/on/hdfs/to/file`
   *
   * Note, that non-local HDFS paths are not supported in this method, e.g. hdfs://nameservice123:8020/path/on/hdfs/too.
   *
   * @param pathString path to convert to FS and relative path
   * @return FS + relative path
   * */
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
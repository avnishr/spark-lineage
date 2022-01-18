package com.dispatcher.avnish

import com.dispatcher.avnish.FileDumpLineageDispatcher._
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
import za.co.absa.spline.producer.model.v1_1.{AttrOrExprRef, ExecutionEvent, ExecutionPlan, FunctionalExpression}
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.modelmapper._

import java.net.URI
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}
import scala.concurrent.blocking

class FileDumpLineageDispatcher(filename: String, permission: FsPermission, bufferSize: Int)
  extends LineageDispatcher
    with Logging {

  def this(conf: Configuration) = this(
    filename = conf.getRequiredString(FileNameKey),
    permission = new FsPermission(conf.getOptionalString(FilePermissionsKey).getOrElse(DefaultFilePermission.toShort.toString)),
    bufferSize = conf.getOptionalInt(BufferSizeKey).getOrElse(DefaultBufferSize)
  )


  override def send(plan: ExecutionPlan): Unit = {


    var lineage = new mutable.HashMap[String, ColumnLineageItem]()
    var outputColList = new mutable.ListBuffer[String]()

    val attrIdColNameMap = getColumnAttributeMapping(plan)
    var attrOrExprMap = extractAttrExprMapping(plan)
    val inputColFileMap = getInputFileColMapping(plan)

    plan.operations.other.get.filter(x => x.id.equals(plan.operations.write.childIds(0))).foreach(x => {
      outputColList ++= x.output.get
    })

    //create a lineage for output columns
    outputColList.foreach( col => lineage.put(col, ColumnLineageItem(col, None, col, plan.operations.write.outputSource)))

    // Build a hierarchial list of ids to process
    var tmpIdList = new mutable.ListBuffer[String]()
    var finalIdList = new mutable.ListBuffer[String]()

    tmpIdList ++= plan.operations.write.childIds.toList
    finalIdList ++= plan.operations.write.childIds.toList

    while (tmpIdList.length > 0) {
      var id = tmpIdList.remove(0)
      plan.operations.other.get.filter(x => x.id.equals(id)).foreach(x => {
        tmpIdList ++= x.childIds.get
        finalIdList ++= x.childIds.get
      })
    }
    tmpIdList.clear()

    // Iterate through these plans to extract the lineage
    while (finalIdList.length > 0) {
      var id = finalIdList.remove(0)
      plan.operations.other.get.filter(x => x.name.get.equals("Project") && x.id.equals(id)).foreach(x => {
        var projList = new mutable.ListBuffer[String]()

        outputColList = new ListBuffer()
        outputColList ++= x.output.get

        x.params match {
          case Some(y) if y.isInstanceOf[Map[String, Option[Vector[AttrOrExprRef]]]] => {
            y.asInstanceOf[Map[String, Option[Vector[AttrOrExprRef]]]].foreach(inst => {
              projList ++= inst._2.get.filter(_.isInstanceOf[AttrOrExprRef]).map(attrOrExpr => attrOrExpr match {
                case AttrOrExprRef(None, expr) => expr
                case AttrOrExprRef(attr, None) => attr
              }).flatten
            })
          }
          case _ => None
        }

        var finalProjList = projList.map(col => {
          if (col.startsWith("expr"))
            attrOrExprMap.get(col).get
          else
            col
        })

        val zippedList = finalProjList zip outputColList

        zippedList.foreach(tup => {
          lineage.remove(tup._2) match {
            case Some(ColumnLineageItem(fromColName, inputTableOrFile, columnDest, outputTableOrFile)) => lineage.put(tup._1,
              ColumnLineageItem(tup._1, inputColFileMap.get(tup._1), columnDest, outputTableOrFile))
            case None => None
          }
        })
      })
    }
    // populate the final lineage
    lineage.foreach( item => println(attrIdColNameMap.get(item._2.fromColName).get + "," + item._2.inputTableOrFile.get + ","
                                        + attrIdColNameMap.get(item._2.columnDest).get + "," + item._2.outputTableOrFile))
  }

  private def getInputFileColMapping(plan:ExecutionPlan) ={
    var inputColFileMap = new mutable.HashMap[String, String]()
    // Extract Column and Input File Mapping
    plan.operations.reads.get.foreach( readOper => {
      readOper.output.get.foreach( id => inputColFileMap.put(id, readOper.inputSources.toList(0)))
    })
    inputColFileMap
  }
  private def getColumnAttributeMapping(plan:ExecutionPlan) ={
    val attrIdColNameMap: HashMap[String, String] = new HashMap[String, String]()

    plan.attributes.foreach(seq => {
      seq.foreach(atrribute => {
        attrIdColNameMap.put(atrribute.id, atrribute.name)
      })
    })
    attrIdColNameMap
  }

  private def extractAttrExprMapping(plan:ExecutionPlan) ={
    // Plan -> Option[Expressions] -> Option[Seq[FunctionalExpression]] -> Option[Seq[AttrOrChildRefs]]
    // We need to extract column Alias
    var attrOrExprMap = new HashMap[String, String]()
    plan.expressions.get.functions.foreach(x => {
      // This gives us a Functional Expression
      x.foreach(fexpr => {
        // extract alias column name mapping
        if (fexpr.extra.get.toList.filter(ptr => ptr._2.toString.contains("Alias")).length > 0)
          fexpr.childRefs match {
            case None => None
            case Some(obj) => obj.filter(c => c.__attrId != None).foreach(attr =>
              attrOrExprMap.put(fexpr.id, attr.__attrId.get))
          }
      })
    })
    attrOrExprMap
  }

  private def persistToHadoopFs(content: String, fullLineagePath: String): Unit = blocking {
    val (fs, path) = pathStringToFsWithPath(fullLineagePath)
    logDebug(s"Opening HadoopFs output stream to $path")

    val replication = fs.getDefaultReplication(path)
    val blockSize = fs.getDefaultBlockSize(path)
    val outputStream = fs.create(path, permission, true, bufferSize, replication, blockSize, null)

    val umask = FsPermission.getUMask(fs.getConf)
    FsPermission.getFileDefault.applyUMask(umask)

    logDebug(s"Writing lineage to $path")
    using(outputStream) {
      _.write(content.getBytes("UTF-8"))
    }
  }

  override def send(event: ExecutionEvent): Unit = {

    //    print(event)

  }
}

object FileDumpLineageDispatcher {

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
package com.github.choppythelumberjack.kettlequill

import java.sql.{ResultSet, SQLException}

import com.github.choppythelumberjack.kettlequill.util.BaseDataMetaStepExt
import org.pentaho.di.core.database.Database
import org.pentaho.di.core.{Const, RowMetaAndData, RowSet}
import org.pentaho.di.core.exception.{KettleDatabaseException, KettleException}
import org.pentaho.di.core.row.{RowDataUtil, RowMeta, RowMetaInterface}
import org.pentaho.di.core.util.Utils
import org.pentaho.di.i18n.BaseMessages
import org.pentaho.di.trans.{Trans, TransMeta}
import org.pentaho.di.trans.step._
import com.github.choppythelumberjack.kettlequill.util.MessageHelper._
import com.github.choppythelumberjack.kettlequill.util.OptionExtensions._

class QuillInput(
  stepMeta: StepMeta,
  stepDataInterface: StepDataInterface,
  copyNr: Int,
  transMeta: TransMeta,
  trans: Trans)
  extends BaseStep(stepMeta, stepDataInterface, copyNr, transMeta, trans) with StepInterface with BaseDataMetaStepExt
{
  var meta: QuillInputMeta = null
  var data: QuillInputData = null


  @throws[KettleException]
  private def readStartDate = {
    if (log.isDetailed) logDetailed("Reading from step [" + data.infoStream.getStepname + "]")
    val parametersMeta = new RowMeta
    var parametersData = new Array[AnyRef](0)
    val rowSet = findInputRowSet(data.infoStream.getStepname)
    if (rowSet != null) {
      var rowData = getRowFrom(rowSet) // rows are originating from "lookup_from"
      while ( {
        rowData != null
      }) {
        parametersData = RowDataUtil.addRowData(parametersData, parametersMeta.size, rowData)
        parametersMeta.addRowMeta(rowSet.getRowMeta)
        rowData = getRowFrom(rowSet) // take all input rows if needed!

      }
      if (parametersMeta.size == 0) throw new KettleException("Expected to read parameters from step [" + data.infoStream.getStepname + "] but none were found.")
    }
    else throw new KettleException("Unable to find rowset to read from, perhaps step [" + data.infoStream.getStepname + "] doesn't exist. (or perhaps you are trying a preview?)")
    val parameters = new RowMetaAndData(parametersMeta, parametersData)
    parameters
  }


  @throws[KettleException]
  override def processRow(smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
    if (first) { // we just got started
      var parameters:Array[AnyRef] = null
      var parametersMeta: RowMetaInterface = null
      first = false
      // Make sure we read data from source steps...
      if (data.infoStream.getStepMeta != null) {
        if (meta.isExecuteEachInputRow) {
          if (log.isDetailed) logDetailed("Reading single row from stream [" + data.infoStream.getStepname + "]")
          data.rowSet = findInputRowSet(data.infoStream.getStepname)
          if (data.rowSet == null) throw new KettleException("Unable to find rowset to read from, perhaps step [" + data.infoStream.getStepname + "] doesn't exist. (or perhaps you are trying a preview?)")
          parameters = getRowFrom(data.rowSet)
          parametersMeta = data.rowSet.getRowMeta
        }
        else {
          if (log.isDetailed) logDetailed("Reading query parameters from stream [" + data.infoStream.getStepname + "]")
          val rmad = readStartDate // Read values in lookup table (look)
          parameters = rmad.getData
          parametersMeta = rmad.getRowMeta
        }
        if (parameters != null) if (log.isDetailed) logDetailed("Query parameters found = " + parametersMeta.getString(parameters))
      }
      else {
        parameters = new Array[AnyRef](0)
        parametersMeta = new RowMeta
      }
      if (meta.isExecuteEachInputRow && (parameters == null || parametersMeta.size == 0)) {
        setOutputDone() // signal end to receiver(s)

        return false // stop immediately, nothing to do here.

      }
      val success = doQuery(parametersMeta, parameters)
      if (!success) return false
    }
    else if (data.thisrow != null) { // We can expect more rows
      try
        data.nextrow = data.db.getRow(data.rs, meta.isLazyConversionActive)
      catch {
        case e: KettleDatabaseException =>
          if (e.getCause.isInstanceOf[SQLException] && isStopped) { //This exception indicates we tried reading a row after the statment for this step was cancelled
            //this is expected and ok so do not pass the exception up
            logDebug(e.getMessage)
            return false
          }
          else throw e
      }
      if (data.nextrow != null) incrementLinesInput
    }
    if (data.thisrow == null) { // Finished reading?
      var done = false
      if (meta.isExecuteEachInputRow) { // Try to get another row from the input stream
        val nextRow = getRowFrom(data.rowSet)
        if (nextRow == null) { // Nothing more to get!
          done = true
        }
        else { // First close the previous query, otherwise we run out of cursors!
          closePreviousQuery()
          val success = doQuery(data.rowSet.getRowMeta, nextRow) // OK, perform a new query
          if (!success) return false
          if (data.thisrow != null) {
            putRow(data.rowMeta, data.thisrow) // fill the rowset(s). (wait for empty)

            data.thisrow = data.nextrow
            if (checkFeedback(getLinesInput)) if (log.isBasic) logBasic("linenr " + getLinesInput)
          }
        }
      }
      else done = true
      if (done) {
        setOutputDone()
        return false // end of data or error.

      }
    }
    else {
      putRow(data.rowMeta, data.thisrow)
      data.thisrow = data.nextrow
      if (checkFeedback(getLinesInput)) if (log.isBasic) logBasic("linenr " + getLinesInput)
    }
    true
  }

  @throws[KettleDatabaseException]
  private def closePreviousQuery(): Unit = {
    if (data.db != null) data.db.closeQuery(data.rs)
  }

  @throws[KettleDatabaseException]
  private def doQuery(parametersMeta: RowMetaInterface, parameters: Array[AnyRef]) = {
    var success = true
    // Open the query with the optional parameters received from the source steps.
    val sql:String = meta.sql.get
    if (log.isDetailed) logDetailed("SQL query : " + sql)
    if (parametersMeta.isEmpty) data.rs = data.db.openQuery(sql, null, null, ResultSet.FETCH_FORWARD, meta.isLazyConversionActive)
    else data.rs = data.db.openQuery(sql, parametersMeta, parameters, ResultSet.FETCH_FORWARD, meta.isLazyConversionActive)
    if (data.rs == null) {
      logError("Couldn't open Query [" + sql + "]")
      setErrors(1)
      stopAll()
      success = false
    }
    else { // Keep the metadata
      data.rowMeta = data.db.getReturnRowMeta
      // Set the origin on the row metadata...
      if (data.rowMeta != null) {
        import scala.collection.JavaConversions._
        for (valueMeta <- data.rowMeta.getValueMetaList) {
          valueMeta.setOrigin(getStepname)
        }
      }
      // Get the first row...
      data.thisrow = data.db.getRow(data.rs)
      if (data.thisrow != null) {
        incrementLinesInput
        data.nextrow = data.db.getRow(data.rs)
        if (data.nextrow != null) incrementLinesInput
      }
    }
    success
  }


  override def dispose(smi: StepMetaInterface, sdi: StepDataInterface): Unit = {
    if (log.isBasic) logBasic("Finished reading query, closing connection.")
    try
      closePreviousQuery()
    catch {
      case e: KettleException =>
        logError("Unexpected error closing query : " + e.toString)
        setErrors(1)
        stopAll()
    } finally if (data.db != null) data.db.disconnect()
    super.dispose(smi, sdi)
  }


  /** Stop the running query */
  @throws[KettleException]
  override def stopRunning(smi: StepMetaInterface, sdi: StepDataInterface): Unit = {
    meta = smi.asInstanceOf[QuillInputMeta]
    data = sdi.asInstanceOf[QuillInputData]
    setStopped(true)
    if (data.db != null && !data.isCanceled) {
      data.db synchronized data.db.cancelQuery()

      data.isCanceled = true
    }
  }


  override def init(smi: StepMetaInterface, sdi: StepDataInterface): Boolean = {
    meta = smi.asInstanceOf[QuillInputMeta]
    data = sdi.asInstanceOf[QuillInputData]
    if (super.init(smi, sdi)) { // Verify some basic things first...
      //
      var passed = true
      if (meta.sql.isEmptyOrEmptyString) {
        logError("TableInput.Exception.SQLIsNeeded"@>)
        passed = false
      }
      if (meta.databaseMeta.isEmpty) {
        logError("TableInput.Exception.DatabaseConnectionsIsNeeded"@>)
        passed = false
      }
      if (!passed) return false
      data.infoStream = meta.getStepIOMeta.getInfoStreams.get(0)
      if (meta.databaseMeta.isEmpty) {
        logError("TableInput.Init.ConnectionMissing"@>)
        return false
      }
      data.db = new Database(this, meta.databaseMeta.get)
      data.db.shareVariablesWith(this)
      data.db.setQueryLimit(Const.toInt(environmentSubstitute(meta.getRowLimit), 0))
      try {
        if (getTransMeta.isUsingUniqueConnections) getTrans synchronized data.db.connect(getTrans.getTransactionId, getPartitionID)
        else data.db.connect(getPartitionID)
        if (meta.databaseMeta.get.isRequiringTransactionsOnQueries) data.db.setCommit(100) // needed for PGSQL it seems...
        if (log.isDetailed) logDetailed("Connected to database...")
        return true
      } catch {
        case e: KettleException =>
          logError("An error occurred, processing will be stopped: " + e.getMessage)
          setErrors(1)
          stopAll()
      }
    }
    false
  }


  def isWaitingForData:Boolean = true
}

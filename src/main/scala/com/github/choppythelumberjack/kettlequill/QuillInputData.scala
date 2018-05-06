package com.github.choppythelumberjack.kettlequill

import java.sql.ResultSet

import org.pentaho.di.core.RowSet
import org.pentaho.di.core.database.Database
import org.pentaho.di.core.row.RowMetaInterface
import org.pentaho.di.trans.step.BaseStepData
import org.pentaho.di.trans.step.StepDataInterface
import org.pentaho.di.trans.step.errorhandling.StreamInterface

// TODO Remove variable stubstitution
class QuillInputData extends BaseStepData with StepDataInterface {
  var nextrow: Array[AnyRef] = null
  var thisrow: Array[AnyRef] = null
  var db: Database = null
  var rs: ResultSet = null
  var lookupStep: String = null
  var rowMeta: RowMetaInterface = null
  var rowSet: RowSet = null
  var isCanceled = false
  var infoStream: StreamInterface = null
}

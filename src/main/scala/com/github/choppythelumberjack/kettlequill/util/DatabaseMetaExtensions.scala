package com.github.choppythelumberjack.kettlequill.util

import org.pentaho.di.core.database.{Database, DatabaseMeta}
import org.pentaho.di.ui.trans.step.BaseStepDialog

object DatabaseMetaExtensions {
  implicit class DatabaseMetaExt(databaseMeta:DatabaseMeta) {
    def toDatabase =
      new Database(BaseStepDialog.loggingObject, databaseMeta)
  }
}

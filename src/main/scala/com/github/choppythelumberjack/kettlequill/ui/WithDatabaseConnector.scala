package com.github.choppythelumberjack.kettlequill.ui

import com.github.choppythelumberjack.kettlequill.HasChangeTracking
import com.github.choppythelumberjack.trivialgen.ext.DatabaseTypes.DatabaseType
import org.eclipse.swt.SWT
import org.eclipse.swt.custom.CCombo
import org.eclipse.swt.events.{ModifyEvent, ModifyListener}
import org.eclipse.swt.widgets.{Composite, Control, MessageBox, Shell}
import org.pentaho.di.core.Const
import org.pentaho.di.core.database.{Database, DatabaseMeta}
import org.pentaho.di.trans.step.BaseStep
import com.github.choppythelumberjack.kettlequill.util.MessageHelper._
import com.github.choppythelumberjack.kettlequill.util.EventLambdaHelper._
import com.github.choppythelumberjack.kettlequill.util._
import org.pentaho.di.core.logging.LogChannel
import org.pentaho.di.ui.trans.step.BaseStepDialog

import scala.util.{Failure, Success, Try}

trait WithDatabaseConnector extends { this: HasMeta with HasChangeTracking with WithScriptPanel =>
  implicit def shellProvider:ShellProvider

  def getMiddlePct:Int

  def wConnection:CCombo = _wConnection
  protected def getLog:LogChannel
  var _wConnection:CCombo = null
  //protected var log:LogChannel

  def addConnectionLine(parent: Composite, previous: Control, middle: Int, margin: Int): CCombo

  def databaseType:Either[Message, DatabaseType] = Try({
    for {
      m <- findDatabaseMeta.right
      dt <- findDatabaseType(m.toDatabase).right
    } yield (dt)
  }) match {
    case Success(v) => v
    case Failure(e) => Left(Message("Failure Finding Database Type", e))
  }

  protected def findDatabaseMeta =
    Option(getTransMeta.findDatabase(wConnection.getText)) match {
      case Some(meta) => Right(meta)
      case None => Left(new Message("Could not get Meta for Connection", s"Error: ${wConnection.getText}"))
    }

  implicit class DatabaseMetaExt(databaseMeta:DatabaseMeta) {
    def toDatabase =
      new Database(BaseStepDialog.loggingObject, databaseMeta)
  }

  def getShell:org.eclipse.swt.widgets.Shell
  def getWStepname:org.eclipse.swt.widgets.Text
  def getTransMeta:org.pentaho.di.trans.TransMeta

  def initDatabaseConnector() = {
    val middle = getMiddlePct
    val margin = Const.MARGIN

    val lsMod:ModifyListener = (e:ModifyEvent) => {
      markChangedInDialog(false) // for prompting if dialog is simply closed
      meta.setChanged()
    }

    // Connection line
    _wConnection = addConnectionLine(getShell, getWStepname, middle, margin)
    if (meta.databaseMeta.isEmpty && getTransMeta.nrDatabases == 1) wConnection.select(0)
    wConnection.addModifyListener(lsMod)

    val datasourceMode:ModifyListener = (e:ModifyEvent) => {
      //println("Running Datasource Modification Listener")
      markChangedInDialog(false) // for prompting if dialog is simply closed
      meta.setChanged()
      (
        for {
          dm <- TryEither.fromEither(findDatabaseMeta)
          dt <- TryEither.fromEither(databaseType)
        } yield (
            (dm, dt)
          )
        ).value match {
        case Success(Right((dm, dt))) => {
          meta.databaseType = Some(dt)
          println(s"Database Type is now ${meta.databaseType}")
          // this is thworing a null pointer exception?
          new SchemaTreeBuilder(dt, dm.toDatabase, getLog).builder match {
            case Some(builder) => builder(tree)
            case None => getLog.logBasic("Cannot rebuild schema tree because data source not available")
          }
          markSqlPaneDirty()
        }
        case Success(Left(msg)) => MessageBoxHelper.error(msg.pushTitle("Could not update datasource"))
        case Failure(e) => MessageBoxHelper.error(Message("Exception Thrown during meta retrieval", e))
      }
    }

    wConnection.addModifyListener(datasourceMode)
  }

  private def findDatabaseType(db:Database):Either[Message, DatabaseType] = {
    try {
      db.connect()
      val productName = db.getConnection.getMetaData.getDatabaseProductName
      val databaseType = DatabaseType.fromProductName(productName)
      Right(databaseType)
    } catch {
      case e:Exception => {
        Left(new Message("Could not get Database Type", s"Exception: ${e.getMessage}"))
      }
    } finally {
      db.disconnect()
    }
  }

}

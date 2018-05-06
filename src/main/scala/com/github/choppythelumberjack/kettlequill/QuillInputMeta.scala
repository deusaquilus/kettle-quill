package com.github.choppythelumberjack.kettlequill

import java.util

import org.pentaho.di.core.CheckResult
import org.pentaho.di.core.CheckResultInterface
import org.pentaho.di.core.Const
import org.pentaho.di.core.annotations.Step
import org.pentaho.di.core.injection.InjectionSupported
import org.pentaho.di.core.util.Utils
import org.pentaho.di.core.database.Database
import org.pentaho.di.core.database.DatabaseMeta
import org.pentaho.di.core.exception.KettleDatabaseException
import org.pentaho.di.core.exception.KettleException
import org.pentaho.di.core.exception.KettlePluginException
import org.pentaho.di.core.exception.KettleStepException
import org.pentaho.di.core.exception.KettleXMLException
import org.pentaho.di.core.row.RowDataUtil
import org.pentaho.di.core.row.RowMeta
import org.pentaho.di.core.row.RowMetaInterface
import org.pentaho.di.core.row.ValueMetaInterface
import org.pentaho.di.core.row.value.ValueMetaFactory
import org.pentaho.di.core.variables.VariableSpace
import org.pentaho.di.core.xml.XMLHandler
import org.pentaho.di.i18n.BaseMessages
import org.pentaho.di.repository.ObjectId
import org.pentaho.di.repository.Repository
import org.pentaho.di.shared.SharedObjectInterface
import org.pentaho.di.trans.DatabaseImpact
import org.pentaho.di.trans.Trans
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.trans.step.BaseStepMeta
import org.pentaho.di.trans.step.StepDataInterface
import org.pentaho.di.trans.step.StepIOMeta
import org.pentaho.di.trans.step.StepIOMetaInterface
import org.pentaho.di.trans.step.StepInjectionMetaEntry
import org.pentaho.di.trans.step.StepInterface
import org.pentaho.di.trans.step.StepMeta
import org.pentaho.di.trans.step.StepMetaInterface
import org.pentaho.di.trans.step.errorhandling.Stream
import org.pentaho.di.trans.step.errorhandling.StreamIcon
import org.pentaho.di.trans.step.errorhandling.StreamInterface
import org.pentaho.di.trans.step.errorhandling.StreamInterface.StreamType
import org.pentaho.metastore.api.IMetaStore
import org.w3c.dom.Node
import java.util.{List => JList}
import com.github.choppythelumberjack.kettlequill.util.MessageHelper._

import com.github.choppythelumberjack.kettlequill.util.StringExtensions._
import com.github.choppythelumberjack.trivialgen.ext.DatabaseTypes.DatabaseType

import scala.annotation.meta.beanSetter
import scala.beans.BeanProperty

@Step(
  id = "QuillStep",
  name = "QuillStep.Name",
  description = "DemoStep.TooltipDesc",
  image = "com/github/choppythelumberjack/tableinput/resources/demo.png",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Input",
  i18nPackageName = "com.github.choppythelumberjack.tableinput",
  documentationUrl = "QuillStep.DocumentationURL",
  casesUrl = "QuillStep.CasesURL",
  forumUrl = "QuillStep.ForumURL"
)
class QuillInputMeta extends BaseStepMeta with StepMetaInterface {

  var databaseMeta: Option[DatabaseMeta] = None
  @BeanProperty
  var sql: Option[String] = None
  private var rowLimit:String = null

  /** Should I execute once per row? */
  private var executeEachInputRow = false
  private var lazyConversionActive = false

  var databaseType:Option[DatabaseType] = None
  var quillQuery:Option[String] = None

  /**
    * @return Returns true if the step should be run per row
    */
  def isExecuteEachInputRow: Boolean = executeEachInputRow

  /**
    * @param oncePerRow
    * true if the step should be run per row
    */
  def setExecuteEachInputRow(oncePerRow: Boolean): Unit = {
    this.executeEachInputRow = oncePerRow
  }

  /**
    * @return Returns the rowLimit.
    */
  def getRowLimit: String = rowLimit

  /**
    * @param rowLimit
    * The rowLimit to set.
    */
  def setRowLimit(rowLimit: String): Unit = {
    this.rowLimit = rowLimit
  }


  @throws(classOf[KettleException])
  def generateSQL(quillQuery:String): String = null

  override def clone():Object = {
    super.clone().asInstanceOf[QuillInputMeta]
  }

  override def setDefault(): Unit = {
    quillQuery = Some("default blah blah")
  }

  protected def getDatabase: Option[Database] =
    databaseMeta.map(new Database(BaseStepMeta.loggingObject, _))

  @throws[KettleStepException]
  override def getFields(row: RowMetaInterface, origin: String, info: Array[RowMetaInterface], nextStep: StepMeta, space: VariableSpace, repository: Repository, metaStore: IMetaStore): Unit = {
    if (getDatabase.isEmpty) throw new IllegalArgumentException("No Database Selected")

    var param = false
    val db = getDatabase.get
    databases = Array[Database](db) // keep track of it for canceling purposes...

    // First try without connecting to the database... (can be S L O W)
    var sNewSQL = sql.orNull
    var add: RowMetaInterface = null
    try
      add = db.getQueryFields(sNewSQL, param)
    catch {
      case dbe: KettleDatabaseException =>
        throw new KettleStepException("Unable to get queryfields for SQL: " + Const.CR + sNewSQL, dbe)
    }
    if (add != null) {
      var i = 0
      while ( {
        i < add.size
      }) {
        val v = add.getValueMeta(i)
        v.setOrigin(origin)

        {
          i += 1; i - 1
        }
      }
      row.addRowMeta(add)
    }
    else try {
      db.connect()
      var paramRowMeta: RowMetaInterface = null
      var paramData:Array[AnyRef] = null
      val infoStream = getStepIOMeta.getInfoStreams.get(0)
      if (!Utils.isEmpty(infoStream.getStepname)) {
        param = true
        if (info.length >= 0 && info(0) != null) {
          paramRowMeta = info(0)
          paramData = RowDataUtil.allocateRowData(paramRowMeta.size)
        }
      }
      add = db.getQueryFields(sNewSQL, param, paramRowMeta, paramData)
      if (add == null) return
      var i = 0
      while ( {
        i < add.size
      }) {
        val v = add.getValueMeta(i)
        v.setOrigin(origin)

        {
          i += 1; i - 1
        }
      }
      row.addRowMeta(add)
    } catch {
      case ke: KettleException =>
        throw new KettleStepException("Unable to get queryfields for SQL: " + Const.CR + sNewSQL, ke)
    } finally db.disconnect()
    if (lazyConversionActive) {
      var i = 0
      while ( {
        i < row.size
      }) {
        val v = row.getValueMeta(i)
        try
            if (v.getType == ValueMetaInterface.TYPE_STRING) {
              val storageMeta = ValueMetaFactory.cloneValueMeta(v)
              storageMeta.setStorageType(ValueMetaInterface.STORAGE_TYPE_NORMAL)
              v.setStorageMetadata(storageMeta)
              v.setStorageType(ValueMetaInterface.STORAGE_TYPE_BINARY_STRING)
            }
        catch {
          case e: KettlePluginException =>
            throw new KettleStepException("Unable to clone meta for lazy conversion: " + Const.CR + v, e)
        }

        {
          i += 1; i - 1
        }
      }
    }
  }


  @throws[KettleXMLException]
  override def loadXML(stepnode: Node, databases: JList[DatabaseMeta], metaStore: IMetaStore): Unit = {
    readData(stepnode, databases)
  }

  @throws[KettleXMLException]
  private def readData(stepnode: Node, databases: JList[_ <: SharedObjectInterface]): Unit = {
    try {
      databaseMeta = Some( DatabaseMeta.findDatabase(databases, XMLHandler.getTagValue(stepnode, "connection")) )
      quillQuery = Some( XMLHandler.getTagValue(stepnode, "quillQuery") )
      sql = Option( XMLHandler.getTagValue(stepnode, "sql") )
      setRowLimit( XMLHandler.getTagValue(stepnode, "limit") )
      val lookupFromStepname = XMLHandler.getTagValue(stepnode, "lookup")
      val infoStream = getStepIOMeta.getInfoStreams.get(0)
      infoStream.setSubject(lookupFromStepname)
      setExecuteEachInputRow( "Y" == XMLHandler.getTagValue(stepnode, "execute_each_row") )
      lazyConversionActive = "Y" == XMLHandler.getTagValue(stepnode, "lazy_conversion_active")
    } catch {
      case e: Exception =>
        throw new KettleXMLException("Unable to load step info from XML", e)
    }
  }

  override def getXML: String = {
    val retval = new StringBuilder
    retval.append("    " + XMLHandler.addTagValue("connection", databaseMeta.map(_.getName).getOrElse("")))

    retval.append("    " + XMLHandler.addTagValue("quillQuery", quillQuery.getOrElse(null)))
    retval.append("    " + XMLHandler.addTagValue("sql", sql.orNull))

    retval.append("    " + XMLHandler.addTagValue("limit", getRowLimit))
    val infoStream = getStepIOMeta.getInfoStreams.get(0)
    retval.append("    " + XMLHandler.addTagValue("lookup", infoStream.getStepname))
    retval.append("    " + XMLHandler.addTagValue("execute_each_row", isExecuteEachInputRow))
    retval.append("    " + XMLHandler.addTagValue("lazy_conversion_active", lazyConversionActive))
    retval.toString
  }

  @throws[KettleException]
  override def readRep(rep: Repository, metaStore: IMetaStore, id_step: ObjectId, databases: JList[DatabaseMeta]): Unit = {
    try {
      databaseMeta = Some( rep.loadDatabaseMetaFromStepAttribute(id_step, "id_connection", databases) )

      quillQuery = Some( rep.getStepAttributeString(id_step, "quillQuery") )
      sql = Option( rep.getStepAttributeString(id_step, "sql") )

      setRowLimit( rep.getStepAttributeString(id_step, "limit") )
      if (getRowLimit == null) setRowLimit( java.lang.Long.toString(rep.getStepAttributeInteger(id_step, "limit")) )
      val lookupFromStepname = rep.getStepAttributeString(id_step, "lookup")
      val infoStream = getStepIOMeta.getInfoStreams.get(0)
      infoStream.setSubject(lookupFromStepname)
      setExecuteEachInputRow( rep.getStepAttributeBoolean(id_step, "execute_each_row") )
      lazyConversionActive = rep.getStepAttributeBoolean(id_step, "lazy_conversion_active")
    } catch {
      case e: Exception =>
        throw new KettleException("Unexpected error reading step information from the repository", e)
    }
  }

  @throws[KettleException]
  override def saveRep(rep: Repository, metaStore: IMetaStore, id_transformation: ObjectId, id_step: ObjectId): Unit = {
    try {
      rep.saveDatabaseMetaStepAttribute(id_transformation, id_step, "id_connection", databaseMeta.orNull)

      rep.saveStepAttribute(id_transformation, id_step, "quillQuery", quillQuery.getOrElse(null))
      rep.saveStepAttribute(id_transformation, id_step, "sql", sql.orNull)

      rep.saveStepAttribute(id_transformation, id_step, "limit", getRowLimit)
      val infoStream = getStepIOMeta.getInfoStreams.get(0)
      rep.saveStepAttribute(id_transformation, id_step, "lookup", infoStream.getStepname)
      rep.saveStepAttribute(id_transformation, id_step, "execute_each_row", isExecuteEachInputRow)
      rep.saveStepAttribute(id_transformation, id_step, "lazy_conversion_active", lazyConversionActive)
      // Also, save the step-database relationship!
      rep.insertStepDatabase(id_transformation, id_step, databaseMeta.map(_.getObjectId).orNull)
    } catch {
      case e: Exception =>
        throw new KettleException("Unable to save step information to the repository for id_step=" + id_step, e)
    }
  }


  override def check(remarks: JList[CheckResultInterface], transMeta: TransMeta, stepMeta: StepMeta, prev: RowMetaInterface, input: Array[String], output: Array[String], info: RowMetaInterface, space: VariableSpace, repository: Repository, metaStore: IMetaStore): Unit = {
    var cr: CheckResult = null
    if (databaseMeta.isDefined) {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "Connection exists", stepMeta)
      remarks.add(cr)
      val db: Database = new Database(BaseStepMeta.loggingObject, databaseMeta.get)
      db.shareVariablesWith(transMeta)
      databases = Array[Database](db) // keep track of it for canceling purposes...

      try {
        db.connect()
        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "Connection to database OK", stepMeta)
        remarks.add(cr)
        if (sql.isDefined && sql.get.length != 0) {
          cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "SQL statement is entered", stepMeta)
          remarks.add(cr)
        }
        else {
          cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "SQL statement is missing.", stepMeta)
          remarks.add(cr)
        }
      } catch {
        case e: KettleException =>
          cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "An error occurred: " + e.getMessage, stepMeta)
          remarks.add(cr)
      } finally db.disconnect()
    }
    else {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Please select or create a connection to use", stepMeta)
      remarks.add(cr)
    }
    // See if we have an informative step...
    val infoStream: StreamInterface = getStepIOMeta.getInfoStreams.get(0)
    if (!Utils.isEmpty(infoStream.getStepname)) {
      val found =
        (0 to input.length)
        .map(infoStream.getStepname ~ input(_))
        .foldLeft(false)(_ || _)

      if (found) {
        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "Previous step to read info from [" + infoStream.getStepname + "] is found.", stepMeta)
        remarks.add(cr)
      }
      else {
        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Previous step to read info from [" + infoStream.getStepname + "] is not found.", stepMeta)
        remarks.add(cr)
      }
      // Count the number of ? in the SQL string:
      var count: Int = 0
      var i: Int = 0
      while ( {
        i < sql.get.length
      }) {
        var c: Char = sql.get.charAt(i)
        if (c == '\'') { // skip to next quote!
          do {
            i += 1
            c = sql.get.charAt(i)
          } while ( {
            c != '\''
          })
        }
        if (c == '?') count += 1

        {
          i += 1; i - 1
        }
      }
      // Verify with the number of informative fields...
      if (info != null) if (count == info.size) {
        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "This step is expecting and receiving " + info.size + " fields of input from the previous step.", stepMeta)
        remarks.add(cr)
      }
      else {
        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "This step is receiving " + info.size + " but not the expected " + count + " fields of input from the previous step.", stepMeta)
        remarks.add(cr)
      }
      else {
        cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Input step name is not recognized!", stepMeta)
        remarks.add(cr)
      }
    }
    else if (input.length > 0) {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Step is not expecting info from input steps.", stepMeta)
      remarks.add(cr)
    }
    else {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "No input expected, no input provided.", stepMeta)
      remarks.add(cr)
    }
  }

  /**
    * @param steps
    * optionally search the info step in a list of steps
    */
  override def searchInfoAndTargetSteps(steps: JList[StepMeta]): Unit = {
    import scala.collection.JavaConversions._
    for (stream <- getStepIOMeta.getInfoStreams) {
      stream.setStepMeta(StepMeta.findStep(steps, stream.getSubject.asInstanceOf[String]))
    }
  }

  override def getStep(
    stepMeta: StepMeta,
    stepDataInterface: StepDataInterface,
    cnr: Int,
    transMeta: TransMeta,
    trans: Trans) = new QuillInput(stepMeta, stepDataInterface, cnr, transMeta, trans)

  override def getStepData = new QuillInputData

  @throws[KettleStepException]
  override def analyseImpact(
    impact: JList[DatabaseImpact],
    transMeta: TransMeta,
    stepMeta: StepMeta,
    prev: RowMetaInterface,
    input: Array[String],
    output: Array[String],
    info: RowMetaInterface,
    repository: Repository,
    metaStore: IMetaStore): Unit =
  {
    if (stepMeta.getName.equalsIgnoreCase("cdc_cust")) System.out.println("HERE!")
    // Find the lookupfields...
    val out = new RowMeta
    // TODO: (from original) this builds, but does it work in all cases.
    getFields(out, stepMeta.getName, Array[RowMetaInterface](info), null, transMeta, repository, metaStore)
    (0 to out.size())
      .map(out.getValueMeta(_))
      .foreach(outvalue => {
        val ii = new DatabaseImpact(
          DatabaseImpact.TYPE_IMPACT_READ,
          transMeta.getName, stepMeta.getName, databaseMeta.get.getDatabaseName, "",
          outvalue.getName, outvalue.getName, stepMeta.getName, sql.get,
          "read from one or more database tables via SQL statement")
        impact.add(ii)
      })
  }

  override def getUsedDatabaseConnections: Array[DatabaseMeta] = if (databaseMeta != null) Array[DatabaseMeta](databaseMeta.get)
  else super.getUsedDatabaseConnections

  /**
    * @return the lazyConversionActive
    */
  def isLazyConversionActive: Boolean = lazyConversionActive

  /**
    * @param lazyConversionActive
    * the lazyConversionActive to set
    */
  def setLazyConversionActive(lazyConversionActive: Boolean): Unit = {
    this.lazyConversionActive = lazyConversionActive
  }

  /**
    * Returns the Input/Output metadata for this step. The generator step only produces output, does not accept input!
    */
  override def getStepIOMeta: StepIOMetaInterface = {
    if (ioMeta == null) {
      ioMeta = new StepIOMeta(true, true, false, false, false, false)
      val stream = new Stream(StreamType.INFO, null, "TableInputMeta.InfoStream.Description"@>, StreamIcon.INFO, null)
      ioMeta.addStream(stream)
    }
    ioMeta
  }

  override def resetStepIoMeta(): Unit = {
    // Do nothing, don't reset as there is no need to do this.
  }

  /**
    * For compatibility, wraps around the standard step IO metadata
    *
    * @param stepMeta
    * The step where you read lookup data from
    */
  def setLookupFromStep(stepMeta: StepMeta): Unit = {
    getStepIOMeta.getInfoStreams.get(0).setStepMeta(stepMeta)
  }

  /**
    * For compatibility, wraps around the standard step IO metadata
    *
    * @return The step where you read lookup data from
    */
  def getLookupFromStep: StepMeta = getStepIOMeta.getInfoStreams.get(0).getStepMeta

  override def getStepMetaInjectionInterface = new QuillInputMetaInjection(this)

  @throws[KettleException]
  override def extractStepMetadataEntries: JList[StepInjectionMetaEntry] = getStepMetaInjectionInterface.extractStepMetadataEntries
}

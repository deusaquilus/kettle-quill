package com.github.choppythelumberjack.kettlequill

import com.github.choppythelumberjack.kettlequill.ui.{HasMeta, WithDatabaseConnector, WithScriptPanel}
import com.github.choppythelumberjack.kettlequill.util.EventLambdaHelper._
import com.github.choppythelumberjack.kettlequill.util.LayoutUtil._
import com.github.choppythelumberjack.kettlequill.util.MessageHelper._
import com.github.choppythelumberjack.kettlequill.util.QuillGenerator.{GenerationException, GenerationNotQuery, GenerationSuccess}
import com.github.choppythelumberjack.kettlequill.util.{StringExtensions => _, _}
import org.eclipse.swt.SWT
import org.eclipse.swt.custom.CCombo
import org.eclipse.swt.events._
import org.eclipse.swt.layout.{FormData, FormLayout}
import org.eclipse.swt.widgets._
import org.pentaho.di.core.Const
import org.pentaho.di.core.logging.LogChannel
import org.pentaho.di.core.util.Utils
import org.pentaho.di.i18n.BaseMessages
import org.pentaho.di.trans.{TransMeta, TransPreviewFactory}
import org.pentaho.di.trans.step.{BaseStepMeta, StepDialogInterface}
import org.pentaho.di.ui.core.dialog.{EnterNumberDialog, EnterTextDialog, PreviewRowsDialog}
import org.pentaho.di.ui.core.widget.TextVar
import org.pentaho.di.ui.spoon.job.JobGraph
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog
import org.pentaho.di.ui.trans.step.BaseStepDialog

import scala.util.{Right => RightEither}
import scala.util.{Left => LeftEither}
import scala.util.{Right => RightEither}
import scala.util.{Left => LeftEither}


class QuillInputDialog (parent: Shell, in: Any, transMeta: TransMeta, sname: String)
  extends BaseStepDialog(parent, in.asInstanceOf[BaseStepMeta], transMeta, sname)
    with StepDialogInterface
    with BaseDialogStepExt
    with WithScriptPanel
    with HasChangeTracking
    with HasMeta
    with WithDatabaseConnector {

  override implicit def shellProvider: ShellProvider = new ShellProvider { override def getShell: Shell = shell}
  override def getMiddlePct: Int = props.getMiddlePct
  override def getShell: Shell = shell
  override def getWStepname: Text = wStepname
  override def getTransMeta: TransMeta = transMeta

  override protected def getLog: LogChannel = this.log

  def meta:QuillInputMeta = in.asInstanceOf[QuillInputMeta]

  private var wlQuillPanel:Label = null
  private var wlDatefrom:Label = null
  private var wDatefrom:CCombo = null
  private var fdlDatefro:FormData = null
  private var lsDatefrom:Listener = null
  private var wlLimit:Label = null
  private var wLimit:TextVar = null
  private var wlEachRow:Label = null
  private var wEachRow:Button = null
  private var wlLazyConversion:Label = null
  private var wLazyConversion:Button = null
  private var wlPosition:Label = null
  private var fdlPosition:FormData = null
  protected var parentDisplay:Display = null

  def open:String = {
    val parent = getParent
    val display = parent.getDisplay
    parentDisplay = parent.getDisplay

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN)
    props.setLook(shell)
    setShellImage(shell, meta)


    val lsMod:ModifyListener = (e:ModifyEvent) => {
      markChangedInDialog(false) // for prompting if dialog is simply closed
      meta.setChanged()
    }

    changed = meta.hasChanged

    val formLayout = new FormLayout
    formLayout.marginWidth = Const.FORM_MARGIN
    formLayout.marginHeight = Const.FORM_MARGIN

    shell.setLayout(formLayout)
    shell.setText("TID.TableInput"@>)

    val middle = props.getMiddlePct
    val margin = Const.MARGIN

    implicit class TextableExtensions[T <: {def setText(str:String):Unit}](control: T) {
      def withText(text:String) = { control.setText(text); control }
    }

    // Stepname line
    wlStepname = new Label(shell, SWT.RIGHT).withText("TID.StepName"@>).look(props)
    fdlStepname = wlStepname.makeLayout(Left(0,0), Right(middle, -margin), Top(0, margin))
    wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER).withText(stepname).look(props)
    wStepname.addModifyListener(lsMod)
    fdStepname = wStepname.makeLayout(Left(middle,0), Top(0,margin), Right(100,0))

    // create the database-connection line etc...,z
    initDatabaseConnector()

    // Some buttons
    wOK = new Button(shell, SWT.PUSH).withText("System.Button.OK"@>)
    wPreview = new Button(shell, SWT.PUSH).withText("System.Button.Preview"@>)
    wCancel = new Button(shell, SWT.PUSH).withText("System.Button.Cancel"@>)
    setButtonPositions(Array[Button](wOK, wPreview, wCancel), margin, null)

    // Limit meta ...
    wlLimit = new Label(shell, SWT.RIGHT).withText("TID.LimitSize"@>).look(props)
    wlLimit.makeLayout(Left(0,0), Right(middle, -margin), BottomOn(wOK, -2*margin))
    wLimit = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER).look(props)
    wLimit.addModifyListener(lsMod)
    wLimit.makeLayout(Left(middle,0), Right(100,0), BottomOn(wOK, -2*margin))

    // Execute for each row?
    wlEachRow = new Label(shell, SWT.RIGHT).withText("TID.ExecuteForEachRow"@>).look(props)
    wlEachRow.makeLayout(Left(0,0), Right(middle,-margin), BottomOn(wLimit, -margin))
    wEachRow = new Button(shell, SWT.CHECK).look(props)
    wEachRow.makeLayout(Left(middle,0),Right(100,0),BottomOn(wLimit,-margin))
    wEachRow.addSelectionListener((e:SelectionEvent) => meta.setChanged())

    // Read date from...
    wlDatefrom = new Label(shell, SWT.RIGHT).withText("TID.InsertDataFromStep"@>).look(props)
    wlDatefrom.makeLayout(Left(0,0),Right(middle,-margin),BottomOn(wEachRow,-margin))
    wDatefrom = new CCombo(shell, SWT.BORDER).look(props)

    val previousSteps = transMeta.findPreviousSteps(transMeta.findStep(stepname))
    import scala.collection.JavaConversions._
    for (stepMeta <- previousSteps) {
      wDatefrom.add(stepMeta.getName)
    }
    wDatefrom.addModifyListener(lsMod)
    wDatefrom.makeLayout(Left(middle,0), Right(100,0), BottomOn(wEachRow, -margin))

    // Replace variables in SQL? // TODO Need to get way to load variables from variable space via scala api calls

    // Lazy conversion?
    //
    wlLazyConversion = new Label(shell, SWT.RIGHT).withText("TID.LazyConversion"@>).look(props)
    wlLazyConversion.makeLayout(Left(0,0),Right(middle,-margin),BottomOn(wDatefrom,-margin))
    wLazyConversion = new Button(shell, SWT.CHECK).look(props)
    wLazyConversion.makeLayout(Left(middle,0),Right(100,0),BottomOn(wDatefrom,-margin))
    wLazyConversion.addSelectionListener((e:SelectionEvent) => { meta.setChanged(); setSQLToolTip() })
    wlPosition = new Label(shell, SWT.NONE).look(props)
    wlPosition.makeLayout(Left(0,0),Right(100,0),BottomOn(wLazyConversion,-margin))

    // Table line...
    wlQuillPanel = new Label(shell, SWT.NONE).withText("TID.SQL"@>).look(props)
    wlQuillPanel.makeLayout(Left(0,0),TopOn(wConnection,margin*2))

    renderScriptPanel(
      transMeta, shell, props, lsMod,
      Left(0,0), TopOn(wConnection,margin), Right(100,-2*margin), BottomOn(wlPosition,-margin))

    // Text Higlighting
    //wQuillPanel.addLineStyleListener(new SQLValuesHighlight)

    // Add listeners
    lsCancel = (e:Event) => cancel()
    lsPreview = (e:Event) => preview()
    lsOK = (e:Event) => ok()
    lsDatefrom = (e:Event) => setFlags()

    wCancel.addListener(SWT.Selection, lsCancel)
    wPreview.addListener(SWT.Selection, lsPreview)
    wOK.addListener(SWT.Selection, lsOK)
    wDatefrom.addListener(SWT.Selection, lsDatefrom)
    wDatefrom.addListener(SWT.FocusOut, lsDatefrom)


    lsDef = new SelectionAdapter() {
      override def widgetDefaultSelected(e: SelectionEvent): Unit = ok()
    }

    wStepname.addSelectionListener(lsDef)
    wLimit.addSelectionListener(lsDef)

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(new ShellAdapter() {
      override def shellClosed(e: ShellEvent): Unit = checkCancel(e)
    })

    syncControlsToMeta()
    wStepname.selectAll()
    wStepname.setFocus

    markChangedInDialog(false) // for prompting if dialog is simply closed

    meta.setChanged(changed)

    // Set the shell size, based upon previous time...
    setSize()

    shell.open()
    while ( {
      !shell.isDisposed
    }) if (!display.readAndDispatch) display.sleep
    return stepname
  }

  override def addConnectionLine(parent: Composite, previous: Control, middle: Int, margin: Int): CCombo =
    super.addConnectionLine(parent, previous, middle, margin)

  private def ok(): Unit = {
    if (Utils.isEmpty(wStepname.getText)) return
    stepname = wStepname.getText // return value

    // copy info to TextFileInputMeta class (meta)
    syncControlsToMeta()
    if (meta.databaseMeta.isEmpty) {
      MessageBoxHelper.error(Message("TID.SelectValidConnection"@>, "TID.DialogCaptionError"@>))
      return
    }

    syncSqlOrMessage()
    if (meta.sql.isEmpty) {
      return
    }

    dispose()
  }





  protected def setSQLToolTip(): Unit = {
    // TODO This should be the compile datatype of what is currently selected
    //if (wVariables.getSelection)
    //  wQuillPanel.setToolTipText(transMeta.environmentSubstitute(wQuillPanel.getText))
  }

  def setPosition(): Unit = {
    val scr = wQuillPanel.getText
    val linenr = wQuillPanel.getLineAtOffset(wQuillPanel.getCaretOffset) + 1
    var posnr = wQuillPanel.getCaretOffset
    // Go back from position to last CR: how many positions?
    var colnr = 0
    while ( {
      posnr > 0 && scr.charAt(posnr - 1) != '\n' && scr.charAt(posnr - 1) != '\r'
    }) {
      posnr -= 1
      colnr += 1
    }
    wlPosition.setText(BaseMessages.getString(PKG, "TID.Position.Label", "" + linenr, "" + colnr))
  }

  private def checkCancel(e: ShellEvent): Unit = {
    if (changedInDialog) {
      val save = JobGraph.showChangedWarning(shell, wStepname.getText)
      if (save == SWT.CANCEL) e.doit = false
      else if (save == SWT.YES) ok()
      else cancel()
    }
    else cancel()
  }

  private def cancel(): Unit = {
    stepname = null
    meta.setChanged(changed)
    dispose()
  }

  def syncMetaToControls(): Unit = {
    if (meta.quillQuery.isDefined) wQuillPanel.setText(meta.quillQuery.get)
    if (meta.databaseMeta.isDefined) wConnection.setText(meta.databaseMeta.get.getName)

    wLimit.setText(Const.NVL(meta.getRowLimit, ""))
    val infoStream = meta.getStepIOMeta.getInfoStreams.get(0)
    if (infoStream.getStepMeta != null) {
      wDatefrom.setText(infoStream.getStepname)
      wEachRow.setSelection(meta.isExecuteEachInputRow)
    }
    else {
      wEachRow.setEnabled(false)
      wlEachRow.setEnabled(false)
    }
    wLazyConversion.setSelection(meta.isLazyConversionActive)
    setSQLToolTip()
    setFlags()
  }

  private def setFlags(): Unit = {
    if (!Utils.isEmpty(wDatefrom.getText)) { // The foreach check box...
      wEachRow.setEnabled(true)
      wlEachRow.setEnabled(true)
      // The preview button...
      wPreview.setEnabled(false)
    }
    else {
      wEachRow.setEnabled(false)
      wEachRow.setSelection(false)
      wlEachRow.setEnabled(false)
      wPreview.setEnabled(true)
    }
  }

  private def syncControlsToMeta() = {
    println("Getting Info")

    meta.databaseType = databaseType.right.toOption
    meta.quillQuery = quillQuery

    meta.databaseMeta = Some(transMeta.findDatabase(wConnection.getText))
    meta.setRowLimit(wLimit.getText)
    val infoStream = meta.getStepIOMeta.getInfoStreams.get(0)
    infoStream.setStepMeta(transMeta.findStep(wDatefrom.getText))
    meta.setExecuteEachInputRow(wEachRow.getSelection)
    meta.setLazyConversionActive(wLazyConversion.getSelection)
  }

  private def preview(): Unit = { // Create the table input reader step...
    import OptionExtensions._
    val oneMeta = new QuillInputMeta

    val output =
      for {
        q <- oneMeta.quillQuery.toEither(Message("Cannot Generate Preview", "Quill Query Not Found")).right
        dt <- this.databaseType.right
        sqlQueryAndType <- generateSqlManually.right
      } yield (q, dt, sqlQueryAndType.sql, sqlQueryAndType.tpe)

    output match {
      case RightEither((qq, dt, sql, tpe)) =>
        oneMeta.quillQuery = Some(qq)
        oneMeta.databaseType = Some(dt)
        oneMeta.sql = Some(sql)
      case LeftEither(msg) =>
        MessageBoxHelper.error(msg)
        return
    }

    val previewMeta = TransPreviewFactory.generatePreviewTransformation(transMeta, oneMeta, wStepname.getText)
    val numberDialog = new EnterNumberDialog(shell, props.getDefaultPreviewSize, BaseMessages.getString(PKG, "TID.EnterPreviewSize"), BaseMessages.getString(PKG, "TID.NumberOfRowsToPreview"))
    val previewSize = numberDialog.open
    if (previewSize > 0) {
      val progressDialog = new TransPreviewProgressDialog(shell, previewMeta, Array[String](wStepname.getText), Array[Int](previewSize))
      progressDialog.open
      val trans = progressDialog.getTrans
      val loggingText = progressDialog.getLoggingText
      if (!progressDialog.isCancelled) if (trans.getResult != null && trans.getResult.getNrErrors > 0) {
        val etd = new EnterTextDialog(shell, BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"), BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"), loggingText, true)
        etd.setReadOnly()
        etd.open
      }
      else {
        val prd = new PreviewRowsDialog(shell, transMeta, SWT.NONE, wStepname.getText, progressDialog.getPreviewRowsMeta(wStepname.getText), progressDialog.getPreviewRows(wStepname.getText), loggingText)
        prd.open()
      }
    }
  }

  private def syncSqlOrMessage() = {
    val sql = generateSqlManually match {
      case RightEither(queryAndType) => Some(queryAndType.sql)
      case LeftEither(message) => {
        MessageBoxHelper.error(message)
        None
      }
    }
    meta.sql = sql
  }
}




































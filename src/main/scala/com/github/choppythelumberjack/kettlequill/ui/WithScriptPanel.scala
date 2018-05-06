package com.github.choppythelumberjack.kettlequill.ui

import com.github.choppythelumberjack.kettlequill.QuillInputMeta
import com.github.choppythelumberjack.kettlequill.util.LayoutUtil._
import org.eclipse.swt.SWT
import org.eclipse.swt.events._
import org.eclipse.swt.widgets._
import org.pentaho.di.core.{Const, Props}
import org.pentaho.di.ui.core.widget.StyledTextComp
import com.github.choppythelumberjack.kettlequill.util.EventLambdaHelper._
import com.github.choppythelumberjack.kettlequill.util.QuillGenerator.{GenerationException, GenerationNotQuery, GenerationSuccess}
import com.github.choppythelumberjack.kettlequill.util.{Message, QueryRegenerator, QuillGenerator}
import com.github.choppythelumberjack.trivialgen.ext.DatabaseTypes.DatabaseType
import org.eclipse.swt.custom.{CTabFolder, CTabItem, SashForm}
import org.pentaho.di.trans.TransMeta
import org.pentaho.di.ui.core.PropsUI
import org.pentaho.di.ui.trans.steps.tableinput.SQLValuesHighlight
import com.github.choppythelumberjack.kettlequill.util.OptionExtensions._
import scala.util.{Right => RightEither}
import scala.util.{Left => LeftEither}

trait WithScriptPanel {

  protected var wQuillPanel:StyledTextComp = null
  protected var tabs:CTabFolder = null
  protected var sqlGenComp:StyledTextComp = null
  protected var parentDisplay:Display
  protected var tree:Tree = null

  def meta:QuillInputMeta
  def databaseType:Either[Message, DatabaseType]

  protected val queryRegenerator = new QueryRegenerator()

  def setPosition(): Unit
  protected def setSQLToolTip(): Unit

  def tabItem(folder:CTabFolder, title:String) = {
    val item = new CTabItem(folder, SWT.NORMAL)
    item.setText(title)
    item
  }

  // probably should not be used out this context, should be invoked
  // from inner thread or some kind of refresh event
  private[this] def updateDirtySqlPane() = {
    queryRegenerator.doRegenIfNeeded(System.currentTimeMillis(), (value) => {
      meta.quillQuery = Some(value)

      // syncExec - seems to run into deadlock sto
      parentDisplay.asyncExec(new Runnable() {
        override def run(): Unit = {
          if (!sqlGenComp.isDisposed) {
            sqlGenComp.setText(value)
            sqlGenComp.getParent.layout
          }
        }
      }) //sqlGenComp.setText(value)
    })
  }

  def markSqlPaneDirty() = {
    meta.quillQuery = Some(wQuillPanel.getText)
    if (meta.quillQuery.isDefined && meta.databaseType.isDefined) {
      println("Enter Update Section")
      queryRegenerator.markDirty(meta.quillQuery.get, meta.databaseType.get, System.currentTimeMillis())
    }
  }

  def renderScriptPanel(
    transMeta: TransMeta,
    shell:Shell,
    props:PropsUI,
    lsMod:ModifyListener,
    modifilers:LayoutModifier*): Unit =
  {

    tabs = new CTabFolder(shell, SWT.BORDER | SWT.COLOR_WHITE)
    tabs.makeLayout(modifilers:_*)

    val quillTabSash = new SashForm(tabs, SWT.VERTICAL)
    val quillTab = tabItem(tabs, "Script")
    wQuillPanel = new StyledTextComp(
      transMeta,
      quillTabSash,
      SWT.MULTI | SWT.LEFT | SWT.H_SCROLL | SWT.V_SCROLL, ""
    ).look(props,Props.WIDGET_STYLE_FIXED)
    //quillTab.setControl(wQuillPanel)

    sqlGenComp = new StyledTextComp(
      transMeta,
      quillTabSash,
      SWT.MULTI | SWT.LEFT | SWT.H_SCROLL | SWT.V_SCROLL, ""
    ).look(props,Props.WIDGET_STYLE_FIXED)

    sqlGenComp.setText("")
    // Text Highlighting
    sqlGenComp.addLineStyleListener(new SQLValuesHighlight)
    sqlGenComp.setEditable(false)


    quillTabSash.setWeights(Array(3,1))
    quillTab.setControl(quillTabSash)


    val schemaTab = tabItem(tabs, "Schema")
    val schemaTabSash = new SashForm(tabs, SWT.HORIZONTAL)



    tree = new Tree(schemaTabSash, SWT.CHECK | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL)
    //(1 to 10).foreach(i => {val t = new TreeItem(tree, SWT.None); t.setText(s"Tree Item ${i}")})

    val schemasComp = new StyledTextComp(
      transMeta,
      schemaTabSash,
      SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, ""
    ).look(props,Props.WIDGET_STYLE_FIXED)
    //schemaTab.setControl(wQuillPanel)


    schemaTabSash.setWeights(Array(1,2))
    schemaTab.setControl(schemaTabSash)

    tabs.setSelection(0)

    addListeners(lsMod)
    markSqlPaneDirty()
  }



  def addListeners(lsMod:ModifyListener) = {
    wQuillPanel.addModifyListener(lsMod)
    wQuillPanel.addModifyListener((e:ModifyEvent) => {setSQLToolTip(); setPosition()})

    wQuillPanel.addModifyListener(new ModifyListener {
      override def modifyText(e: ModifyEvent): Unit = {
        markSqlPaneDirty()
      }
    })


    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          Thread.sleep(1000)
          updateDirtySqlPane()
        }
      }
    }).start()

//    wQuillPanel.addListener(SWT.Paint, new Listener {
//      override def handleEvent(event: Event): Unit = {
//        println("Redraw")
//        updateDirtySqlPane()
//      }
//    })

    wQuillPanel.addKeyListener(new KeyAdapter() {
      override def keyPressed(e: KeyEvent): Unit = setPosition()
      override def keyReleased(e: KeyEvent): Unit = setPosition()
    })
    wQuillPanel.addFocusListener(new FocusAdapter() {
      override def focusGained(e: FocusEvent): Unit = setPosition()
      override def focusLost(e: FocusEvent): Unit = setPosition()
    })
    wQuillPanel.addMouseListener(new MouseAdapter() {
      override def mouseDoubleClick(e: MouseEvent): Unit = setPosition()
      override def mouseDown(e: MouseEvent): Unit = setPosition()
      override def mouseUp(e: MouseEvent): Unit = setPosition()
    })
  }

  def quillQuery:Option[String] = Option(wQuillPanel.getText)

  // TODO Should not be all or none, should have an applicative functor
  def generateSqlManually = {
    def RightE(queryAndType: QueryAndType) = RightEither[Message, QueryAndType](queryAndType)
    def LeftE(m:Message) = LeftEither[Message, QueryAndType](m)

    for {
      quillQuery <- quillQuery.toEither(Message("Cannot Evaluate Query", "Quill Query is not Defined")).right
      databaseType <- databaseType.right
      queryOrError <- {
        QuillGenerator(quillQuery, databaseType) match {
          case GenerationSuccess(sql, tpe) =>
            RightE(QueryAndType(sql, tpe))
          case GenerationNotQuery(tpe:String) =>
            LeftE(Message("Result Not a Quill Query", s"Result was '${tpe}' which is not a quill query"))
          case GenerationException(e) =>
            LeftE(Message("Quill Query Generation Error", "Query Query Generation Experienced and Error:" + Const.CR + e.getMessage))
        }
      }.right
    } yield (queryOrError)
  }
}

case class QueryAndType(sql:String, tpe:String)
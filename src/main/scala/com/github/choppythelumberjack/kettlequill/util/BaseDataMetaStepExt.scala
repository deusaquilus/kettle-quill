package com.github.choppythelumberjack.kettlequill.util

import com.github.choppythelumberjack.kettlequill.{QuillInputData, QuillInputMeta}
import org.pentaho.di.trans.step.BaseStep
import org.pentaho.di.ui.trans.step.BaseStepDialog

trait BaseDialogStepExt { this:BaseStepDialog =>
  def meta:QuillInputMeta

  def logIfDetailed(str:String) = logDetailed(str)
}

trait BaseDataMetaStepExt { this:BaseStep =>
  def meta:QuillInputMeta
  def data:QuillInputData

  def logIfDetailed(str:String) = logDetailed(str)
  def stepname = data.infoStream.getStepname
  def findMyInputRowSet = Option(findInputRowSet(stepname))
}

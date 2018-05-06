package com.github.choppythelumberjack.kettlequill.util

import com.github.choppythelumberjack.kettlequill.QuillInputMeta
import org.pentaho.di.i18n.BaseMessages

object MessageHelper {
  val PKG = classOf[QuillInputMeta]

  implicit class StringExtensions(str:String) {
    def @> = BaseMessages.getString( PKG, str )
  }
}

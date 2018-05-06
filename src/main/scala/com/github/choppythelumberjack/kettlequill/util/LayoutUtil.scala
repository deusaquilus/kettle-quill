package com.github.choppythelumberjack.kettlequill.util

import org.eclipse.swt.layout.{FormAttachment, FormData}
import org.pentaho.di.ui.core.PropsUI

object LayoutUtil {
  sealed trait LayoutModifier
  case class Left(location:Int, margin:Int) extends LayoutModifier
  case class LeftOn(control:org.eclipse.swt.widgets.Control, margin:Int) extends LayoutModifier
  case class Right(location:Int, margin:Int) extends LayoutModifier
  case class RightOn(control:org.eclipse.swt.widgets.Control, margin:Int) extends LayoutModifier
  case class Top(location:Int, margin:Int) extends LayoutModifier
  case class TopOn(control:org.eclipse.swt.widgets.Control, margin:Int) extends LayoutModifier
  case class Bottom(location:Int, margin:Int) extends LayoutModifier
  case class BottomOn(control:org.eclipse.swt.widgets.Control, margin:Int) extends LayoutModifier

  implicit class ControlExtensions[T <: org.eclipse.swt.widgets.Control](control: T) {
    def makeLayout(steps:LayoutModifier*):FormData = {
      val fd = new FormData
      steps.foreach {
        case Left(loc, m) => fd.left = new FormAttachment(loc, m)
        case LeftOn(ctrl, m) => fd.left = new FormAttachment(ctrl, m)
        case Right(loc, m) => fd.right = new FormAttachment(loc, m)
        case RightOn(ctrl, m) => fd.right = new FormAttachment(ctrl, m)
        case Top(loc, m) => fd.top = new FormAttachment(loc, m)
        case TopOn(ctrl, m) => fd.top = new FormAttachment(ctrl, m)
        case Bottom(loc, m) => fd.bottom = new FormAttachment(loc, m)
        case BottomOn(ctrl, m) => fd.bottom = new FormAttachment(ctrl, m)
      }
      control.setLayoutData(fd)
      fd
    }
    def look(ui:PropsUI) = { ui.setLook(control); control }
    def look(ui:PropsUI, style:Int) = { ui.setLook(control, style); control }
  }
}

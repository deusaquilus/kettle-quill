package com.github.choppythelumberjack.kettlequill.util

import org.eclipse.swt.SWT
import org.eclipse.swt.widgets.MessageBox

trait ShellProvider {
  def getShell:org.eclipse.swt.widgets.Shell
}

case class Message(title:String, content:String, exception: Option[Throwable]) {
  def this(title:String, content:String) = this(title, content, None)
  def this(title:String, exception: Throwable) = this(title, "", Some(exception))

  def pushTitle(newTitle:String) = new Message(newTitle, s"${title} ${content}", exception)
}

object Message {
  def apply(title:String, content:String) = new Message(title, content)
  def apply(title:String, exception: Throwable) = new Message(title, exception)
}

object MessageBoxHelper {
  def error(message:Message)(implicit shellProvider:ShellProvider): Unit = {
    val mb = new MessageBox(shellProvider.getShell, SWT.OK | SWT.ICON_ERROR)
    mb.setText(message.title)
    mb.setMessage(message.content + message.exception.map("\n" + _.getMessage).getOrElse(""))
    mb.open
  }
}

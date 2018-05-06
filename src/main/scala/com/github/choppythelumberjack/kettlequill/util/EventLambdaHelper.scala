package com.github.choppythelumberjack.kettlequill.util

import org.eclipse.swt.events.{ModifyEvent, ModifyListener, SelectionAdapter, SelectionEvent}
import org.eclipse.swt.widgets.{Event, Listener}

object EventLambdaHelper {
  implicit def modifyLambdaExtensions(handleFunc:(ModifyEvent) => Unit):ModifyListener = {
    new ModifyListener() {
      override def modifyText(e: ModifyEvent): Unit = handleFunc(e)
    }
  }
  implicit def eventLambdaExtensions(handleFunc:Event => Unit):Listener = {
    new Listener() {
      override def handleEvent(e: Event): Unit = handleFunc(e)
    }
  }
  implicit def selectionEventLambdaExtensions(handleFunc:SelectionEvent => Unit): SelectionAdapter = {
    new SelectionAdapter() {
      override def widgetSelected(e: SelectionEvent): Unit = handleFunc(e)
    }
  }
}

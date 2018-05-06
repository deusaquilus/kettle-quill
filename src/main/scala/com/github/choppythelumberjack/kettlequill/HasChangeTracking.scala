package com.github.choppythelumberjack.kettlequill

trait HasChangeTracking {

  private var _changedInDialog:Boolean = false

  def markChangedInDialog(value:Boolean) = {
    _changedInDialog = value
  }

  def changedInDialog = _changedInDialog
}

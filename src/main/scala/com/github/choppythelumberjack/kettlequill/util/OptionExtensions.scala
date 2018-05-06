package com.github.choppythelumberjack.kettlequill.util

object OptionExtensions {
  implicit class StringOptionExtensions(opt:Option[String]) {
    def isEmptyOrEmptyString = opt.isEmpty || opt.get.trim == ""
  }

  implicit class OptionExtensions[T](opt:Option[T]) {
    def orThrow(e:Throwable):T = opt match {
      case Some(value) => value
      case None => throw e
    }
    def ifExists(method : =>Unit) = {
      if (opt.isDefined) method
      opt
    }
    def ifNotExists(method : =>Unit) = {
      if (opt.isEmpty) method
      opt
    }
    def toEither[U](alternative:U) =
      Either.cond[U, T](opt.isDefined, opt.get, alternative)
  }
}

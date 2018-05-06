package com.github.choppythelumberjack.kettlequill.util

object StringExtensions {
  implicit class ImplicitStringExtensions(str:String) {
    def ~(other:String) = str.equalsIgnoreCase(other)
  }
}

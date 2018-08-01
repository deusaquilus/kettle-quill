package com.github.choppythelumberjack.kettlequill

object EitherExtensions {
  implicit class EitherExtensions[L, R](eth:Either[L, R]) {
    def orDoSomething(action:L => Unit) =
      if (eth.isLeft) {
        action(eth.left.get)
        None
      }
      else
        Some(eth.right.get)
  }
}

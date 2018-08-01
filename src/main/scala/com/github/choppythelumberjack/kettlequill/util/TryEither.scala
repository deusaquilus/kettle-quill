package com.github.choppythelumberjack.kettlequill.util

import scala.util.{Try, Success, Failure, Either, Left, Right}

case class TryEither[L,R](value:Try[Either[L,R]]) {
  def map[B](f: R=>B): TryEither[L,B] = TryEither(value.map(eth => eth.right.map(f(_))))
  def flatMap[B](f: R=>TryEither[L,B]): TryEither[L,B] = value match {
    case Success(Right(v)) => f(v)
    case Success(Left(v)) => TryEither(Success(Left[L,B](v)))
    case Failure(e) => TryEither(Failure[Either[L,B]](e))
  }
}

object TryEither {
  def pure[L,R](value:R) = new TryEither(Success(Right[L,R](value)))
  def pureLeft[L,R](value:L) = new TryEither(Success(Left[L,R](value)))
  def fromEither[L,R](e:Either[L,R]) = new TryEither(Success(e))
  def fromTry[T](t:Try[T]) =
    t match {
      case Success(v) => pure[Nothing,T](v)
      case Failure(e) => new TryEither(Failure[Either[Nothing, T]](e))
    }
}

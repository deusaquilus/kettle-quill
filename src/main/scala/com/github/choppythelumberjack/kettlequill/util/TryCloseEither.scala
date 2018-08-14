package com.github.choppythelumberjack.kettlequill.util

import com.github.choppythelumberjack.kettlequill.schemagen.SchemaCodegen
import com.github.choppythelumberjack.tryclose._
import com.github.choppythelumberjack.tryclose.JavaImplicits._
import org.pentaho.di.core.database.Database

case class TryCloseEither[L,R:CanClose](value:TryClose[Either[L,R]]) {
  import TryCloseEither.Implicits._

  def map[B](f: R=>B)(implicit evidence:CanClose[B]): TryCloseEither[L,B] =
    TryCloseEither(value.map(eth => eth.right.map(f(_))))

  // R => TryCloseEither(TryClose[Either[L, R]])
  // eth: Either[L, R] -> f[R->TryCloseEither[R, B]
  // case Left[L] => wrap(Same)
  // case Right[R] => f(_)
  def flatMap[B](f: R => TryCloseEither[L,B])(implicit evidence:CanClose[B]): TryCloseEither[L,B] = {
    val mapped =
      value.flatMap({
        case Right(value) => f(value).value
        case Left(value) => TryClose(Left[L, B](value)) //maybe do type casting instead of re-wrap? What we we rely on type erasure?
      })
    TryCloseEither(mapped)
  }
}

object TryCloseEither {
  import com.github.choppythelumberjack.tryclose._
  import com.github.choppythelumberjack.tryclose.JavaImplicits._

  object Implicits {
    implicit def closeableEither[L, R:CanClose]:CanClose[Either[L, R]] =
      new CanClose[Either[L, R]] {
      override def close(closeable:Either[L, R]):Unit = closeable match {
        case Right(value) => implicitly[CanClose[R]].close(value)
        case Left(_) =>
      }
    }

    implicit def closeableEitherRight[L, R:CanClose]:CanClose[Right[L, R]] =
      new CanClose[Right[L, R]]{
      override def close(closeable:Right[L, R]):Unit = closeable match {
        case Right(value) => implicitly[CanClose[R]].close(value)
      }
    }
    implicit class CloseablePentahoDatabase(db:Database) extends CanClose[Database] {
      override def close(db: Database): Unit = {
        db.disconnect()
      }
    }
  }

  import Implicits._

  def pureNonCloseable[L,R](value:R) = new TryCloseEither(TryClose(Right[L,Wrapper[R]](Wrapper(value))))
  def pureLeftNonCloseable[L,R](value:L) = new TryCloseEither(TryClose(Left[L,Wrapper[R]](value)))

  def pure[L,R:CanClose](value:R) = new TryCloseEither(TryClose(Right[L,R](value)))
  def pureLeft[L,R:CanClose](value:L) = new TryCloseEither(TryClose(Left[L,R](value)))
  def fromEither[L,R:CanClose](e:Either[L,R]) = new TryCloseEither(TryClose(e))
  def fromTryClose[T:CanClose](t:TryClose[T]) = TryCloseEither(TryClose(Right(t)))
  def fromNonCloseableEither[L,R](e:Either[L,R]): TryCloseEither[L, Wrapper[R]] =
    TryCloseEither.fromEither(e.right.map(r => Wrapper(r)))
}

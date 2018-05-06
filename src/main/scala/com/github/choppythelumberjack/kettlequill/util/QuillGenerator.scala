package com.github.choppythelumberjack.kettlequill.util

import com.github.choppythelumberjack.trivialgen.ext.DatabaseTypes.DatabaseType

import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try => RTry}

class InvalidTypeException(invalidType:String) extends Exception {
}


object QuillGenerator {

    // TODO Is MirrorSqlDialect a valid thing that you can use?

  def makeScript(quillQuery:String) =
s"""
import io.getquill._
val ctx = new MirrorContext(MySQLDialect, UpperCase)
import ctx._

${quillQuery}
"""



  sealed trait QuillGeneratorStatus
  case class GenerationSuccess(sql:String, actualType:String) extends QuillGeneratorStatus
  case class GenerationException(e:Throwable) extends QuillGeneratorStatus
  case class GenerationNotQuery(actualType:String) extends QuillGeneratorStatus


  def apply(quillQuery:String, databaseType:DatabaseType):QuillGeneratorStatus = {
    //TODO Choose namine strategy based on database
    //  -all lowercase for postgres
    //  -all upercase for mysql
    //  -let the user chose? i.e. make a menu

    // TODO Create some sort of check to make sure that quillQuery actually returns a quill query
    // Alternatively, take a look at the type of value it is in the script engine and act accordingly
    // e.g. if it's a scalar, return a scalar sql value (what about in the preview pane?)

    val script = makeScript(quillQuery)


    import scala.reflect.runtime.universe._
    import scala.tools.reflect.ToolBox

    val tb = runtimeMirror(this.getClass.getClassLoader).mkToolBox()
    val ob = tb.parse(script)

    object MyTransformer extends Transformer {
      def putIntoRun(tree: universe.Tree): universe.Tree = {
        q"run($tree).string"
      }
      override def transform(tree: universe.Tree): universe.Tree = {
        tree match {
          case b :Block => //.dropRight(1)
            // TODO Expr needs to be query type or throw exception
            treeCopy.Block(b, b.stats, putIntoRun(b.expr))
          case other => other
        }
      }
    }

    val transformed = MyTransformer.transform(ob)

    //BaseStepMeta.loggingObject
    //println("============================\n"+transformed+"\n============================")

    //val result = tb.compile(ob)()
    //val result = tb.compile(transformed)()
    //println(result)


    case class CompileResult(result:Any, tpe:String)

    RTry( {
      val startTime = System.currentTimeMillis()
      val out = tb.eval(transformed)
//      val tpe = RTry({ob.tpe.toString}) match {
//        case Success(value) => value
//        case Failure(e) => s"Could not typecheck: ${e.getMessage.split("\n").head}"
//      }

      val tpe = "Foo Bar"

      println(s"Compile Time: ${System.currentTimeMillis() - startTime}")
      CompileResult(out, tpe)
    } ) match {
      case scala.util.Success(CompileResult(result:String, tpe)) => GenerationSuccess(result, tpe)
      case scala.util.Success(CompileResult(value, tpe)) => GenerationNotQuery(tpe)
      case scala.util.Failure(e) => GenerationException(e)
    }
  }
}

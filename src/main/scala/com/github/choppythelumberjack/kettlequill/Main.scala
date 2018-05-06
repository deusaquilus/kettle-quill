package com.github.choppythelumberjack.kettlequill

import io.getquill.SqlMirrorContext
import io.getquill.ast.Query
import javax.script.{Bindings, ScriptEngineManager, SimpleBindings}

import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.settings.MutableSettings
import javax.script.ScriptContext
import javax.script.SimpleScriptContext
import javax.script.ScriptContext.ENGINE_SCOPE
import javax.script.ScriptContext.GLOBAL_SCOPE

import scala.reflect.runtime.universe


object Main {
  def main(args: Array[String]): Unit = {
    //    val engine = new ScriptEngineManager().getEngineByName("scala");
    //    engine.asInstanceOf[IMain]
    //      .settings.usejavacp.asInstanceOf[MutableSettings#BooleanSetting].value_$eq(true)

    val script =
      s"""
import io.getquill._
val ctx = new MirrorContext(MySQLDialect, CamelCase)
import ctx._

def f[A](a: A)(implicit evA: scala.reflect.runtime.universe.WeakTypeTag[A]) = evA

def isQuery[A](a: A)(implicit evA: scala.reflect.runtime.universe.WeakTypeTag[A]) = evA.tpe <:< scala.reflect.runtime.universe.typeOf[ctx.Quoted[_ <: Any]]

case class Person(first_name:String, last_name:String, age:Int)
quote { query[Person].filter(_.age > 22) }
"""

    import scala.reflect.runtime.universe._
    import scala.tools.reflect.ToolBox

    val tb = runtimeMirror(this.getClass.getClassLoader).mkToolBox()

    var startTime = System.currentTimeMillis()
    val ob = tb.parse(script)

    object MyTransformer extends Transformer {
      def putIntoRun(tree: universe.Tree): universe.Tree = {
        q"(isQuery($tree), f($tree).tpe, run($tree).string)"
      }

      //def runCommand = q"run(q).string"

      override def transform(tree: universe.Tree): universe.Tree = {
        tree match {
          case b: Block => //.dropRight(1)
            treeCopy.Block(b, b.stats, putIntoRun(b.expr))
          case other => other
        }
      }
    }


    val transformed = MyTransformer.transform(ob)


    startTime = System.currentTimeMillis()
    val result = tb.compile(transformed)().asInstanceOf[(Boolean, AnyRef, AnyRef)]

    val isQuery = result._2.asInstanceOf[RefinedType].parents.last.typeArgs.head.<:<(typeOf[io.getquill.MirrorIdiom])

    println(s"Compile Time: ${System.currentTimeMillis() - startTime}")


    //val outType = tb.untypecheck(ob.asInstanceOf[Block])
    //val lastExp = outType.children.last
    //val outTypeTpe = tb.typecheck(lastExp)
    //println(s"Out Type: ${outType.tpe}")

    println(result)

    val res = result

    //ob.children.foreach(println(_))


  }
}
package com.github.choppythelumberjack.kettlequill.util

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.github.choppythelumberjack.kettlequill.util.QuillGenerator.{GenerationException, GenerationNotQuery, GenerationSuccess}
import com.github.choppythelumberjack.tableinput.formatter.SqlFormatter
import com.github.choppythelumberjack.trivialgen.ext.DatabaseTypes.DatabaseType

//import scala.concurrent.ExecutionContext.Implicits.global
import monix.eval.Task
import monix.execution.CancelableFuture
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

class QueryRegenerator {
  var currentQuill:AtomicReference[String] = new AtomicReference[String](null)
  var currDatabaseType:AtomicReference[DatabaseType] = new AtomicReference[DatabaseType](null)

  var futureQuill:AtomicReference[String] = new AtomicReference[String](null)
  var futureDatabaseType:AtomicReference[DatabaseType] = new AtomicReference[DatabaseType](null)
  var lastUpdate:AtomicLong = new AtomicLong(0)

  var task:AtomicReference[Option[CancelableFuture[Unit]]] = new AtomicReference[Option[CancelableFuture[Unit]]](None)

  def markDirty(sql:String, databaseType:DatabaseType, time:Long):Unit = {
    //println("Adding Update Task")
    this.synchronized {
      futureQuill.set(sql)
      futureDatabaseType.set(databaseType)
      lastUpdate.set(time)
    }
  }

  def nullOrEmpty(str:String) = (str == null || str.trim == "")

  def doRegenIfNeeded(time:Long, callback:(String) => Unit):Unit = {
    // if less then 3 seconds have passed, don't update anything
    val timeDiff = (time - lastUpdate.get())
    //println(s"Time Diff: ${timeDiff}")
    if (timeDiff < 1000) return
    //println("Date Diff Passed")

    this.synchronized {
      if (nullOrEmpty(currentQuill.get) && nullOrEmpty(futureQuill.get)) {
        callback("-- Enter a Query")
        return
      }

      if (currDatabaseType.get == null && futureDatabaseType == null) {
        callback("-- No Database Detected. Cannot Preview Query")
        return
      }

      if (futureQuill.get == currentQuill.get && futureDatabaseType.get == currDatabaseType.get) {
        //println(s"Returning From from doRegen if futureQuill ${futureQuill.get} same as currentSql ${currentQuill.get} and databaseType ${futureDatabaseType.get} same as currDatabaseType ${currDatabaseType.get}")
        return
      }
      callback("-- Reloading")

      //println("Update Comparison Criteria Passed")

      // if there is a task already running, let it complete (if no task it's completed by default)
      // POSSIBLE FEATURE In future check what the future sql of the task is and cancel it if it's not the same???
//      if (task.get.map(t => !t.isCompleted).getOrElse(false)) {
//        println("A future is already in progress")
//        return
//      }

      if (task.get.isDefined) {
        task.get.get.cancel()
      }

      //println("Starting Refresh Future")

      // Otherwise start the task to do it
      task.set(Some(Task {
        QuillGenerator(futureQuill.get, futureDatabaseType.get) match {
          case GenerationSuccess(value, tpe) => {
            //println("Future Successful")
            currentQuill.set(futureQuill.get)
            currDatabaseType.set(futureDatabaseType.get)
            lastUpdate.set(time)
            val formatted = new SqlFormatter().format(value)
            callback(s"-- Type: ${tpe}\n" + formatted)
          }
          case GenerationException(e) => {
            //println(s"Generation Exception: ${e.getMessage}")
            currentQuill.set(futureQuill.get)
            currDatabaseType.set(futureDatabaseType.get)
            lastUpdate.set(time)
            callback(s"Generation Exception:\n${e.getMessage}".split("\n").map(s => s"-- ${s}").mkString("\n"))
          }
          case GenerationNotQuery(value) => {
            //println(s"Generation Not a Query ${value}")
            currentQuill.set(futureQuill.get)
            currDatabaseType.set(futureDatabaseType.get)
            lastUpdate.set(time)
            callback(s"Generation Not a Query ${value}")
          }
        }
      }.runAsync))
    }
  }
}

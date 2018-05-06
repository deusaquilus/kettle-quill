import io.getquill._
val ctx = new MirrorContext(MySQLDialect, Literal)
import ctx._

case class Person(first_name:String, last_name:String, age:Int)
val q = quote { query[Person].filter(_.age > 22) }

run(q).string
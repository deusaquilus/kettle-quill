package com.github.choppythelumberjack.kettlequill.util

import java.sql.ResultSet

import com.github.choppythelumberjack.trivialgen.ext.DatabaseTypes.DatabaseType
import org.eclipse.swt.SWT
import org.eclipse.swt.widgets.{Tree, TreeItem}
import org.pentaho.di.core.database.Database
import org.pentaho.di.core.logging.LogChannelInterface
import com.github.choppythelumberjack.kettlequill.EitherExtensions._

object SchemaTreeExtractor {
  import scala.collection.JavaConversions._

  def selectedNodes(tree:Tree) = {
    tree
      .getItems.map(catItem => (catItem, catItem.getItems))
      .filter({case (catItem, schemItem) => (catItem.getChecked)}) // filter for selected schemas
      .map({case (catItem, schemItem) => (catItem, schemItem.filter(_.getChecked)) }) // filter for selected nodes in the schemas
      .map({case (catItem, schemItem) => // get the data inside and cast to correct type
        (
          catItem.getData.asInstanceOf[String],
          schemItem.map(_.asInstanceOf[String])
        )
      })
      // map to only non-null schema values
      .map({case (catItem, schemItem) => (catItem, schemItem)})
  }
}

object SchemaTreeBuilder{
  val ALL_SCHEMAS = "<ALL>"
}

class SchemaTreeBuilder(databaseType: DatabaseType, db:Database, log:LogChannelInterface) {

  protected def pullFromResult[T](rs:ResultSet, extractor:ResultSet=>T, accum:Seq[T] = Seq()) =
    if (!rs.next()) accum.reverse
    else extractor(rs) +: accum

  protected def databasesAndSchemas = {
    import com.github.choppythelumberjack.tryclose._
    import com.github.choppythelumberjack.tryclose.JavaImplicits._

    log.logBasic(s"Attempting to connect to: ${db.getDatabaseMeta.getName} for schema")
    val catAndSchemaData = for {
      _ <- TryClose(db.normalConnect(null))
      conn <- TryClose(db.getConnection) if ({
        if (conn == null) log.logError(s"Cannot connect to db: ${db.getDatabaseMeta.getName}")
        conn != null
      })
      results <- TryClose(conn.getMetaData().getTables(null, null, null, null))
      catAndSchema <- TryClose.wrap(pullFromResult(results,
        r => (Option(r.getString("TABLE_CAT")), Option(r.getString("TABLE_SCHEM")))))
    } yield (catAndSchema)

    catAndSchemaData.unwrap match {
      case Success(catAndSchema) => Right(catAndSchema)
      case Failure(e) => Left(new Message("Could not retrieve schema", Option(e)))
    }

  }

  def builder = {
    val resultOrError =
      databasesAndSchemas
        .orDoSomething(msg => {
          log.logBasic(s"Cannot retrieve datasource/schema: ${msg}")
        })
        .map(
          _.groupBy({ case (cat, schema) => cat }) // group by the catalog (i.e. db)
           .map({ case (k, kv) =>
            // remove empty schema-values from the list of schemas from the catalog
            log.logBasic(s"Schema ${(k, kv)}")
            (k, kv.map(_._2).collect({ case Some(value) => value }))
          })
          .collect({case (Some(cat), schemas) => (cat,schemas)})
          .map({case (cat, schemas) => (cat,SchemaTreeBuilder.ALL_SCHEMAS +: schemas)})
        )


    resultOrError.map(
      groupedSchemas => {
        (root:Tree) => {
          val selected = SchemaTreeExtractor.selectedNodes(root)

          root.clearAll(true)
          root.deselectAll()

          def catTreeItem(content:String, parent:TreeItem) = {
            val t = new TreeItem(parent, SWT.None);
            t.setText(content);
            t.setData(content);

            t.setChecked(selected.exists({case (cat, _) => cat == content})); t
          }
          def schemaTreeItem(content:String, parent:Tree) = {
            val t = new TreeItem(parent, SWT.None);
            t.setText(content);
            t.setData(content);

              t.setChecked(
                // check that the list of previously-selected items contains the catalog/schema combo
                selected.exists({case (cat, schem) =>
                  parent.getData == cat && content.exists(c => schem.contains(c))})
              ); t
          }

          println(s"Grouped Schemas: ${groupedSchemas.toMap}")

          groupedSchemas.foreach({case (cat, schemas) => {
            val catItem = schemaTreeItem(cat, root)
            schemas.foreach(schema => catTreeItem(schema, catItem))
          }})
        }
      }
    )


  }
}

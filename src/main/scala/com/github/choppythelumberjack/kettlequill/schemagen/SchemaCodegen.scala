package com.github.choppythelumberjack.kettlequill.schemagen

import com.github.choppythelumberjack.kettlequill.util.{SchemaTreeBuilder, SchemaTreeExtractor}
import com.github.choppythelumberjack.trivialgen.{ConnectionMaker, TrivialSnakeCaseNames}
import com.github.choppythelumberjack.trivialgen.ext.TrivialGen
import com.github.choppythelumberjack.trivialgen.gen.CodeGeneratorConfig
import com.github.choppythelumberjack.trivialgen.model.TableSchema
import org.eclipse.swt.widgets.Tree
import org.pentaho.di.core.database.Database

object SchemaCodegen {

  def apply(db:Database, tree:Tree):String = {

    val selectedSchemas = SchemaTreeExtractor.selectedNodes(tree)
    val tableSchemaSelectors = selectedSchemas.map({case (cat, schem) => TableSchemaMatcher(cat, schem)})

    println(s"Found Selected Schemas: ${selectedSchemas}")

    case class TableSchemaMatcher(cat:String, schem:Seq[String]) {
      def matches(tc: TableSchema) =
        tc.table.tableCat == cat &&
          (
            schem.contains(SchemaTreeBuilder.ALL_SCHEMAS) || // if ALL_SCHEMAS present select everything in the catalog/db
            Option(tc.table.tableSchem).forall(schem.contains(_)) // null safety using optionals
          )
    }

    // Don't need to pass db credentials into generator because we are creating the connection maker from the pentaho Database object
    val gen = new TrivialGen(CodeGeneratorConfig("", "", ""), "gen") {
      override def connectionMaker(cs: CodeGeneratorConfig): ConnectionMaker = () => {db.getConnection}
      override def namingStrategy = TrivialSnakeCaseNames

      override def filter(tc: TableSchema): Boolean = tableSchemaSelectors.exists(_.matches(tc))
    }

    gen.writeStrings.mkString("\n-- ***************************************\n")
  }
}

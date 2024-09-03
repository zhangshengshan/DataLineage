package io.github.zhangshengshan

import org.apache.calcite.adapter.jdbc.JdbcSchema
import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.jdbc.CalciteConnection
import org.apache.calcite.rel.RelRoot
import org.apache.calcite.tools.Frameworks
import org.apache.commons.dbcp2.BasicDataSource

import java.sql.DriverManager
import java.util.Properties

object FieldLineageExample extends App {

  import org.apache.calcite.sql.parser.SqlParser

  val parserConfig = SqlParser
    .configBuilder(SqlParser.Config.DEFAULT)
    .setCaseSensitive(false)
    .setUnquotedCasing(Casing.UNCHANGED)
    .build()

  val sql =
    """SELECT
      |a.a as fa, a.b as fb, a.c as fc,
      |b.a as fa1 , b.b as fb1 ,b.c as fc1 FROM
      | anneng_ods.a as a left outer join anneng_ods.b as b
      | on a.a = b.a
      | """.stripMargin
  // Parse SQL query
  val parser = SqlParser.create(sql)
  val sqlNode = parser.parseQuery()

  val user = "root"
  val password = "aaaa1111"
  val ip = "127.0.0.1"
  val port = 3306

  val url = s"jdbc:mysql://$ip:$port/anneng_ods"
  val dataSource = new BasicDataSource()
  dataSource.setUrl(url)
  dataSource.setUsername(user)
  dataSource.setPassword(password)
  dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver")

  val connection = DriverManager.getConnection("jdbc:calcite:", new Properties())
  val calciteConnection = connection.unwrap(classOf[CalciteConnection])
  val rootSchema = calciteConnection.getRootSchema

  val schema = rootSchema.add("anneng_ods", JdbcSchema.create(rootSchema, null, dataSource, null, null))

  val config = Frameworks.newConfigBuilder()
    .parserConfig(parserConfig)
    .defaultSchema(rootSchema) // Set default schema
    .build()
  val planner = Frameworks.getPlanner(config)

  println("Starting SQL parsing and validation")

  // Parse and validate the SQL query
  val parsedSqlNode = planner.parse(sql)
  val validatedSqlNode = planner.validate(parsedSqlNode)

  // Generate the query plan
  val relRoot: RelRoot = planner.rel(validatedSqlNode)

  // Print the relational algebra tree
  println(s"Relational Algebra Tree: ${relRoot.rel.explain()}")

  // Traverse query plan, extract table names
  val fieldLineageShuttle: FieldLineageShuttle = new FieldLineageShuttle()
  fieldLineageShuttle.go(relRoot.rel) // Use the go method of RelVisitor

  println("Table extraction complete")
  println(s"Tables found: ${fieldLineageShuttle.tables.mkString(", ")}")
}
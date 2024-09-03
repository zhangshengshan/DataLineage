package io.github.zhangshengshan

import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.calcite.rel.core.TableScan

import scala.collection.JavaConverters._
import scala.collection.mutable

class FieldLineageShuttle extends RelVisitor {
  val tables = mutable.Set[String]()

  override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
    println(s"Visiting node: ${node.getClass.getSimpleName}")
    node match {
      case tableScan: TableScan =>
        val tableName = tableScan.getTable.getQualifiedName.asScala.mkString(".")
        println(s"Found table: $tableName")
        tables += tableName
      case _ =>
        println("Processing other node")
    }
    super.visit(node, ordinal, parent)
  }
}
package io.github.zhangshengshan

import org.apache.calcite.rel.core.{Project, TableScan}
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.calcite.rex.{RexInputRef, RexNode}

import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

class FieldLineageShuttle extends RelVisitor {
  val tables: mutable.Set[String] = mutable.Set[String]()
  val fieldAliases: mutable.Map[String, String] = mutable.Map[String, String]()

  override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
    println(s"Visiting node: ${node.getClass.getSimpleName}")
    node match {
      case tableScan: TableScan =>
        val tableName: String = tableScan.getTable.getQualifiedName.asScala.mkString(".")
        println(s"Found table: $tableName")
        tables += tableName

      case project: Project =>
        println("Processing Project node")
        val input: RelNode = project.getInput
        val projects: util.List[RexNode] = project.getProjects
        val fieldNames: util.List[String] = project.getRowType.getFieldNames

        projects.zip(fieldNames).foreach {
          case (rexNode, fieldName) =>
            rexNode match {
              case inputRef: RexInputRef =>
                val inputFieldName = input.getRowType.getFieldNames.get(inputRef.getIndex)
                println(s"Mapping field: $fieldName -> $inputFieldName")
                fieldAliases += (fieldName -> inputFieldName)
              case _ =>
                println(s"Unhandled RexNode type: ${rexNode.getClass.getSimpleName}")
            }
        }

      case _ =>
        println("Processing other node")
    }
    super.visit(node, ordinal, parent)
  }
}
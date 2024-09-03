package io.github.zhangshengshan

import org.apache.calcite.rel.{RelNode, RelShuttleImpl}
import org.apache.calcite.rel.core.{Project, TableScan}
import org.apache.calcite.rex.{RexInputRef, RexNode}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

class FieldLineageShuttle extends RelShuttleImpl {
  val lineage = mutable.Map[String, String]()

  override def visit(node: RelNode): RelNode = {
    println("Entering visit method")
    println(s"Visiting node: ${node.getClass.getSimpleName}")
    node match {
      case project: Project =>
        println("Processing Project node")
        val input = project.getInput
        val projects = project.getProjects
        val fieldNames = project.getRowType.getFieldNames

        println(s"Project fields: ${fieldNames.mkString(", ")}")
        println(s"Project expressions: ${projects.map(_.toString).mkString(", ")}")

        projects.zip(fieldNames).foreach {
          case (rexNode, fieldName) =>
            rexNode match {
              case inputRef: RexInputRef =>
                val inputFieldName = input.getRowType.getFieldNames.get(inputRef.getIndex)
                println(s"Mapping field: $fieldName -> $inputFieldName")
                lineage += (fieldName -> inputFieldName)
              case _ =>
                println(s"Unhandled RexNode type: ${rexNode.getClass.getSimpleName}")
            }
        }
        super.visit(project)
      case tableScan: TableScan =>
        println("Processing TableScan node")
        super.visit(tableScan)
      case _ =>
        println("Processing other node")
        super.visit(node)
    }
    println(s"Current lineage map: $lineage")
    node
  }
}
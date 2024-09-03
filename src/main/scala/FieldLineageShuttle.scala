package io.github.zhangshengshan

import org.apache.calcite.rel.{RelNode, RelShuttleImpl}
import org.apache.calcite.rel.core.{Project, TableScan}
import org.apache.calcite.rex.RexInputRef

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

class FieldLineageShuttle extends RelShuttleImpl {
  val lineage = mutable.Map[String, String]()

  override def visit(node: RelNode): RelNode = {
    println("00000")
    node match {
      case project: Project =>
        println("11111")
        val input = project.getInput
        val projects = project.getProjects
        val fieldNames = project.getRowType.getFieldNames

        projects.zip(fieldNames).foreach {
          case (rexNode, fieldName) =>
            rexNode match {
              case inputRef: RexInputRef =>
                val inputFieldName = input.getRowType.getFieldNames.get(inputRef.getIndex)
                lineage += (fieldName -> inputFieldName)
              case _ =>
            }
        }
        super.visit(project)
      case tableScan: TableScan =>
        println("22222")
        super.visit(tableScan)
      case _ =>
        println("33333")
        super.visit(node)
    }
  }
}

// 使用自定义的 shuttle 来遍历查询计划并提取字段血缘信息


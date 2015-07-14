/*
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.compiler
package operator
package core

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.ProjectionOperatorUtil
import com.asakusafw.lang.compiler.model.graph.CoreOperator
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class ProjectionOperatorsCompiler extends CoreOperatorCompiler {

  override def support(
    operator: CoreOperator)(
      implicit context: SparkClientCompiler.Context): Boolean = {
    operator.getCoreOperatorKind == CoreOperatorKind.PROJECT ||
      operator.getCoreOperatorKind == CoreOperatorKind.EXTEND ||
      operator.getCoreOperatorKind == CoreOperatorKind.RESTRUCTURE
  }

  override def operatorType: OperatorType = OperatorType.ExtractType

  override def compile(
    operator: CoreOperator)(
      implicit context: SparkClientCompiler.Context): Type = {

    assert(support(operator),
      s"The operator type is not supported: ${operator.getCoreOperatorKind}")
    assert(operator.inputs.size == 1,
      s"The size of inputs should be 1: ${operator.inputs.size}")
    assert(operator.outputs.size == 1,
      s"The size of outputs should be 1: ${operator.outputs.size}")

    val builder = new ProjectionOperatorsFragmentClassBuilder(operator)

    context.jpContext.addClass(builder)
  }
}

private class ProjectionOperatorsFragmentClassBuilder(
  operator: CoreOperator)(
    implicit context: SparkClientCompiler.Context)
  extends CoreOperatorFragmentClassBuilder(
    operator.inputs.head.dataModelType,
    operator.outputs.head.dataModelType)
  with ScalaIdioms {

  val mappings =
    ProjectionOperatorUtil.getPropertyMappings(context.jpContext.getDataModelLoader, operator)
      .toSeq

  override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
    import mb._ // scalastyle:ignore

    thisVar.push().getField("childDataModel", childDataModelType).invokeV("reset")

    mappings.foreach { mapping =>
      val srcProperty = mapping.getSourcePort.dataModelRef
        .findProperty(mapping.getSourceProperty)
      val destProperty = mapping.getDestinationPort.dataModelRef
        .findProperty(mapping.getDestinationProperty)
      assert(srcProperty.getType.asType == destProperty.getType.asType,
        "The source and destination types should be the same: " +
          s"(${srcProperty.getType}, ${destProperty.getType}")

      pushObject(mb)(ValueOptionOps)
        .invokeV(
          "copy",
          dataModelVar.push()
            .invokeV(srcProperty.getDeclaration.getName, srcProperty.getType.asType),
          thisVar.push().getField("childDataModel", childDataModelType)
            .invokeV(destProperty.getDeclaration.getName, destProperty.getType.asType))
    }

    thisVar.push().getField("child", classOf[Fragment[_]].asType)
      .invokeV("add", thisVar.push()
        .getField("childDataModel", childDataModelType)
        .asType(classOf[AnyRef].asType))
    `return`()
  }
}

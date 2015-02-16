package com.asakusafw.spark.compiler
package operator
package core

import scala.collection.JavaConversions._

import com.asakusafw.lang.compiler.model.graph.CoreOperator
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.CoreOperatorCompiler
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._

class ProjectOperatorCompiler extends CoreOperatorCompiler {

  def of: CoreOperatorKind = CoreOperatorKind.PROJECT

  def compile(operator: CoreOperator)(implicit context: Context): FragmentClassBuilder = {
    assert(operator.getCoreOperatorKind == of)

    val inputs = operator.getInputs.toSeq
    assert(inputs.size > 0)
    val input = inputs.head
    assert(inputs.tail.forall(_.getDataType == input.getDataType))
    val inputDataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
    val inputDataModelType = inputDataModelRef.getDeclaration.asType

    val outputs = operator.getOutputs.toSeq
    assert(outputs.size > 0)
    val output = outputs.head
    assert(outputs.tail.forall(_.getDataType == output.getDataType))
    val outputDataModelRef = context.jpContext.getDataModelLoader.load(output.getDataType)
    val outputDataModelType = outputDataModelRef.getDeclaration.asType

    new CoreOperatorFragmentClassBuilder(inputDataModelType, outputDataModelType) {

      override def defAddMethod(mb: MethodBuilder): Unit = {
        import mb._
        val resultVar = `var`(inputDataModelType, thisVar.nextLocal)
        outputDataModelRef.getProperties.foreach { property =>
          val p = inputDataModelRef.findProperty(property.getName)
          assert(p != null)
          assert(p.getType == property.getType)
          val methodName = p.getDeclaration.getName
          val propertyType = p.getType.asType
          thisVar.push().getField("childDataModel", outputDataModelType)
            .invokeV(methodName, propertyType)
            .invokeV("copyFrom",
              resultVar.push().invokeV(methodName, propertyType))
        }
        thisVar.push().getField("child", classOf[Fragment[_]].asType)
          .invokeV("add", thisVar.push().getField("childDataModel", outputDataModelType).asType(classOf[AnyRef].asType))
        `return`()
      }
    }
  }
}

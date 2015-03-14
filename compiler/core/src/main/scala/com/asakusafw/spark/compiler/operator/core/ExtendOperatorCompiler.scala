package com.asakusafw.spark.compiler
package operator
package core

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.CoreOperator
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.CoreOperatorCompiler
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._

class ExtendOperatorCompiler extends CoreOperatorCompiler {

  override def of: CoreOperatorKind = CoreOperatorKind.EXTEND

  override def compile(operator: CoreOperator)(implicit context: Context): Type = {
    assert(operator.getCoreOperatorKind == of)

    // get InputDataModel.
    val inputs = operator.getInputs.toSeq
    assert(inputs.size > 0)
    val input = inputs.head
    assert(inputs.tail.forall(_.getDataType == input.getDataType))
    val inputDataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
    val inputDataModelType = inputDataModelRef.getDeclaration.asType

    // get OutputDataModel.
    val outputs = operator.getOutputs.toSeq
    assert(outputs.size > 0)
    val output = outputs.head
    assert(outputs.tail.forall(_.getDataType == output.getDataType))
    val outputDataModelRef = context.jpContext.getDataModelLoader.load(output.getDataType)
    val outputDataModelType = outputDataModelRef.getDeclaration.asType

    val builder = new CoreOperatorFragmentClassBuilder(context.flowId, inputDataModelType, outputDataModelType) {

      override def defAddMethod(mb: MethodBuilder): Unit = {
        import mb._
        val resultVar = `var`(inputDataModelType, thisVar.nextLocal)

        // ensure that OutputDataModel has all the members of InputDataModel.
        inputDataModelRef.getProperties.foreach { sourceProperty =>
          val p = outputDataModelRef.findProperty(sourceProperty.getName)
          assert(p != null)
          assert(p.getType == sourceProperty.getType)
        }

        // set all the members of output, 
        // especially members not in OutputDataModel, to the initial values.
        thisVar.push().getField("childDataModel", outputDataModelType)
          .invokeV("reset")

        // copy the members from input.
        outputDataModelRef.getProperties.foreach { destProperty =>
          val methodName = destProperty.getDeclaration.getName
          val propertyType = destProperty.getType.asType
          val p = inputDataModelRef.findProperty(destProperty.getName)
          if (p != null) {
            thisVar.push().getField("childDataModel", outputDataModelType)
              .invokeV(methodName, propertyType)
              .invokeV("copyFrom",
                resultVar.push().invokeV(methodName, propertyType))
          }
        }

        thisVar.push().getField("child", classOf[Fragment[_]].asType)
          .invokeV("add", thisVar.push()
            .getField("childDataModel", outputDataModelType)
            .asType(classOf[AnyRef].asType))
        `return`()
      }
    }

    context.jpContext.addClass(builder)
  }
}

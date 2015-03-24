package com.asakusafw.spark.compiler
package operator
package user
package join

import java.util.{ List => JList }

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.analyzer.util.{ JoinedModelUtil, MasterJoinOperatorUtil }
import com.asakusafw.lang.compiler.model.graph.{ OperatorOutput, UserOperator }
import com.asakusafw.runtime.core.Result
import com.asakusafw.spark.compiler.operator.FragmentClassBuilder
import com.asakusafw.spark.compiler.spi.UserOperatorCompiler
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.runtime.operator.DefaultMasterSelection
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.MasterJoin

class MasterJoinOperatorCompiler extends UserOperatorCompiler {

  override def of: Class[_] = classOf[MasterJoin]

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    val annotationDesc = operator.getAnnotation
    assert(annotationDesc.getDeclaringClass.resolve(context.jpContext.getClassLoader) == of)
    val implementationClassType = operator.getImplementationClass.asType

    val selectionMethod =
      Option(MasterJoinOperatorUtil.getSelection(context.jpContext.getClassLoader, operator))
        .map(method => (method.getName, Type.getType(method)))

    val inputs = operator.getInputs.toSeq
    assert(inputs.size >= 2)
    val inputDataModelRefs = inputs.map(input => context.jpContext.getDataModelLoader.load(input.getDataType))
    val inputDataModelTypes = inputDataModelRefs.map(_.getDeclaration.asType)

    val masterDataModelType = inputDataModelTypes(MasterJoin.ID_INPUT_MASTER)
    val txDataModelType = inputDataModelTypes(MasterJoin.ID_INPUT_TRANSACTION)

    val outputs = operator.getOutputs.toSeq
    assert(outputs.size == 2)
    val outputDataModelRefs = outputs.map(output => context.jpContext.getDataModelLoader.load(output.getDataType))
    val outputDataModelTypes = outputDataModelRefs.map(_.getDeclaration.asType)

    val joinedOutput = outputs(MasterJoin.ID_OUTPUT_JOINED)
    val joinedOutputDataModelRef = context.jpContext.getDataModelLoader.load(joinedOutput.getDataType)
    val joinedOutputDataModelType = joinedOutputDataModelRef.getDeclaration.asType

    val missedOutput = outputs(MasterJoin.ID_OUTPUT_MISSED)
    val missedOutputDataModelRef = context.jpContext.getDataModelLoader.load(missedOutput.getDataType)
    val missedOutputDataModelType = missedOutputDataModelRef.getDeclaration.asType

    assert(missedOutputDataModelType == txDataModelType)

    val mappings = JoinedModelUtil.getPropertyMappings(context.jpContext.getClassLoader, operator).toSeq

    val builder = new JoinOperatorClassBuilder(context.flowId) {

      override def operatorType: Type = implementationClassType
      override def operatorOutputs: Seq[OperatorOutput] = outputs

      override def masterType: Type = masterDataModelType
      override def txType: Type = txDataModelType
      override def masterSelection: Option[(String, Type)] = selectionMethod

      override def defFields(fieldDef: FieldDef): Unit = {
        super.defFields(fieldDef)
        fieldDef.newField("joinedDataModel", joinedOutputDataModelType)
      }

      override def defConstructors(ctorDef: ConstructorDef): Unit = {
        ctorDef.newInit((0 until outputs.size).map(_ => classOf[Fragment[_]].asType),
          ((new MethodSignatureBuilder() /: outputs) {
            case (builder, output) =>
              builder.newParameterType {
                _.newClassType(classOf[Fragment[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, output.getDataType.asType)
                }
              }
          })
            .newVoidReturnType()
            .build()) { mb =>
            import mb._
            thisVar.push().invokeInit(superType)
            thisVar.push().putField("joinedDataModel", joinedOutputDataModelType, pushNew0(joinedOutputDataModelType))
            initOutputFields(mb, thisVar.nextLocal)
          }
      }

      override def join(mb: MethodBuilder, ctrl: LoopControl, masterVar: Var, txVar: Var): Unit = {
        import mb._
        masterVar.push().unlessNotNull {
          getOutputField(mb, missedOutput).invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
          ctrl.continue()
        }

        val vars = Seq(masterVar, txVar)

        thisVar.push().getField("joinedDataModel", joinedOutputDataModelType).invokeV("reset")

        mappings.foreach { mapping =>
          val src = inputs.indexOf(mapping.getSourcePort)
          val srcVar = vars(src)
          val srcProperty = inputDataModelRefs(src).findProperty(mapping.getSourceProperty)
          val destProperty = joinedOutputDataModelRef.findProperty(mapping.getDestinationProperty)
          assert(srcProperty.getType.asType == destProperty.getType.asType)

          getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
            .invokeV(
              "copy",
              srcVar.push()
                .invokeV(srcProperty.getDeclaration.getName, srcProperty.getType.asType),
              thisVar.push().getField("joinedDataModel", joinedOutputDataModelType)
                .invokeV(destProperty.getDeclaration.getName, destProperty.getType.asType))
        }

        getOutputField(mb, joinedOutput)
          .invokeV("add", thisVar.push().getField("joinedDataModel", joinedOutputDataModelType).asType(classOf[AnyRef].asType))
      }
    }

    context.jpContext.addClass(builder)
  }
}

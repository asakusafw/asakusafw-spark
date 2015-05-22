package com.asakusafw.spark.compiler
package operator
package user

import java.util.{ List => JList }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.flow.{ ArrayListBuffer, FileMapListBuffer, ListBuffer }
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.flow.processor.InputBuffer
import com.asakusafw.vocabulary.operator.{ CoGroup, GroupSort }

class CoGroupOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[CoGroup] || annotationDesc.resolveClass == classOf[GroupSort]
  }

  override def operatorType: OperatorType = OperatorType.CoGroupType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(support(operator),
      s"The operator type is not supported: ${annotationDesc.resolveClass.getSimpleName}")
    assert(inputs.size > 0,
      s"The size of inputs should be greater than 0: ${inputs.size}")

    assert(
      methodDesc.parameterClasses
        .zip(inputs.map(_ => classOf[JList[_]])
          ++: outputs.map(_ => classOf[Result[_]])
          ++: arguments.map(_.resolveClass))
        .forall {
          case (method, model) => method.isAssignableFrom(model)
        },
      s"The operator method parameter types are not compatible: (${
        methodDesc.parameterClasses.map(_.getName).mkString("(", ",", ")")
      }, ${
        (inputs.map(_ => classOf[JList[_]])
          ++: outputs.map(_ => classOf[Result[_]])
          ++: arguments.map(_.resolveClass)).map(_.getName).mkString("(", ",", ")")
      })")

    val inputBuffer = annotationDesc.getElements()("inputBuffer").resolve(context.jpContext.getClassLoader)
      .asInstanceOf[InputBuffer]

    val builder = new UserOperatorFragmentClassBuilder(
      context.flowId,
      classOf[Seq[Iterator[_]]].asType,
      implementationClassType,
      outputs) {

      override def defFields(fieldDef: FieldDef): Unit = {
        super.defFields(fieldDef)

        fieldDef.newField(Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL, "buffers", classOf[Array[ListBuffer[_]]].asType,
          new TypeSignatureBuilder()
            .newArrayType {
              _.newClassType(classOf[ListBuffer[_]].asType) {
                _.newTypeArgument()
              }
            }
            .build())
      }

      override def initFields(mb: MethodBuilder): Unit = {
        super.initFields(mb)

        import mb._
        thisVar.push().putField("buffers", classOf[Array[ListBuffer[_]]].asType, {
          val arr = pushNewArray(classOf[ListBuffer[_]].asType, inputs.size)
          inputs.zipWithIndex.foreach {
            case (input, i) =>
              arr.dup().astore(ldc(i), pushNew0(
                inputBuffer match {
                  case InputBuffer.EXPAND => classOf[ArrayListBuffer[_]].asType
                  case InputBuffer.ESCAPE => classOf[FileMapListBuffer[_]].asType
                }))
          }
          arr
        })
      }

      override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
        import mb._
        val nextLocal = new AtomicInteger(dataModelVar.nextLocal)

        val bufferVars = inputs.zipWithIndex.map {
          case (input, i) =>
            val iter = dataModelVar.push()
              .invokeI("apply", classOf[AnyRef].asType, ldc(i).box().asType(classOf[AnyRef].asType))
              .cast(classOf[Iterator[_]].asType)
            val iterVar = iter.store(nextLocal.getAndAdd(iter.size))
            val buffer = thisVar.push().getField("buffers", classOf[Array[ListBuffer[_]]].asType).aload(ldc(i))
            val bufferVar = buffer.store(nextLocal.getAndAdd(buffer.size))
            bufferVar.push().invokeI("begin")

            whileLoop(iterVar.push().invokeI("hasNext", Type.BOOLEAN_TYPE)) { ctrl =>
              bufferVar.push().invokeI("isExpandRequired", Type.BOOLEAN_TYPE).unlessFalse {
                bufferVar.push().invokeI("expand", pushNew0(input.dataModelType).asType(classOf[AnyRef].asType))
              }
              bufferVar.push().invokeI("advance", classOf[AnyRef].asType)
                .cast(input.dataModelType)
                .invokeV(
                  "copyFrom",
                  iterVar.push().invokeI("next", classOf[AnyRef].asType)
                    .cast(input.dataModelType))
            }

            bufferVar.push().invokeI("end")
            bufferVar
        }

        getOperatorField(mb)
          .invokeV(
            methodDesc.getName,
            bufferVars.map(_.push().asType(classOf[JList[_]].asType))
              ++ outputs.map { output =>
                getOutputField(mb, output).asType(classOf[Result[_]].asType)
              }
              ++ arguments.map { argument =>
                ldc(argument.value)(ClassTag(argument.resolveClass))
              }: _*)

        val i = ldc(0)
        val iVar = i.store(nextLocal.getAndAdd(i.size))
        loop { ctrl =>
          iVar.push().unlessLessThan(ldc(inputs.size))(ctrl.break())
          thisVar.push().getField("buffers", classOf[Array[ListBuffer[_]]].asType).aload(iVar.push())
            .invokeI("shrink")
          iVar.inc(1)
        }

        `return`()
      }
    }

    context.jpContext.addClass(builder)
  }
}

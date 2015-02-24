package com.asakusafw.spark.compiler.subplan

import java.util.concurrent.atomic.AtomicLong

import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor
import org.apache.spark.Partitioner

import com.asakusafw.spark.runtime.driver.CoGroupDriver
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class CoGroupDriverClassBuilder(
  val branchKeyType: Type,
  val groupingKeyType: Type)
    extends SubPlanDriverClassBuilder(
      Type.getType(s"L${classOf[CoGroupDriver[_, _]].asType.getInternalName}$$${CoGroupDriverClassBuilder.nextId};"),
      Option(CoGroupDriverClassBuilder.signature(branchKeyType, groupingKeyType)),
      classOf[CoGroupDriver[_, _]].asType)
    with BranchKeysField
    with PartitionersField
    with OrderingsField {

  override def defFields(fieldDef: FieldDef): Unit = {
    defBranchKeysField(fieldDef)
    defPartitionersField(fieldDef)
    defOrderingsField(fieldDef)
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newStaticInit { mb =>
      initBranchKeysField(mb)
      initPartitionersField(mb)
      initOrderingsField(mb)
    }

    ctorDef.newInit(Seq(classOf[Seq[_]].asType, classOf[Partitioner].asType, classOf[Ordering[_]].asType)) { mb =>
      import mb._
      val inputsVar = `var`(classOf[Seq[_]].asType, thisVar.nextLocal)
      val partVar = `var`(classOf[Partitioner].asType, inputsVar.nextLocal)
      val groupingVar = `var`(classOf[Ordering[_]].asType, partVar.nextLocal)
      thisVar.push().invokeInit(superType, inputsVar.push(), partVar.push(), groupingVar.push())
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    defBranchKeys(methodDef)
    defPartitioners(methodDef)
    defOrderings(methodDef)
  }
}

object CoGroupDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  def signature(branchKeyType: Type, groupingKeyType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[CoGroupDriver[_, _]].asType) {
          _
            .newTypeArgument(SignatureVisitor.INSTANCEOF, branchKeyType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, groupingKeyType)
        }
      }
      .build()
  }
}

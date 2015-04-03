package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.NameTransformer

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.description.TypeDescription
import com.asakusafw.lang.compiler.model.graph.Group
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._

class ShuffleKeyClassBuilder(
  val flowId: String,
  dataModelType: Type,
  grouping: Seq[(String, Type)],
  ordering: Seq[(String, Type)])
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/ShuffleKey$$${ShuffleKeyClassBuilder.nextId};"),
      classOf[ShuffleKey].asType) {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq.empty) { mb =>
      import mb._
      thisVar.push().invokeInit(
        superType, {
          val builder = getStatic(Vector.getClass.asType, "MODULE$", Vector.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

          grouping.foreach {
            case (_, t) =>
              builder.invokeI(
                NameTransformer.encode("+="),
                classOf[mutable.Builder[_, _]].asType,
                pushNew0(t).asType(classOf[AnyRef].asType))
          }

          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        }, {
          val builder = getStatic(Vector.getClass.asType, "MODULE$", Vector.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

          ordering.foreach {
            case (_, t) =>
              builder.invokeI(
                NameTransformer.encode("+="),
                classOf[mutable.Builder[_, _]].asType,
                pushNew0(t).asType(classOf[AnyRef].asType))
          }

          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        })
    }

    ctorDef.newInit(Seq(dataModelType)) { mb =>
      import mb._
      val dataModelVar = `var`(dataModelType, thisVar.nextLocal)
      thisVar.push().invokeInit()
      thisVar.push().invokeV("copyFrom", dataModelVar.push())
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("copyFrom", Seq(dataModelType)) { mb =>
      import mb._
      val dataModelVar = `var`(dataModelType, thisVar.nextLocal)

      val groupingIterVar = thisVar.push()
        .invokeV("grouping", classOf[Seq[_]].asType)
        .invokeI("iterator", classOf[Iterator[_]].asType).store(dataModelVar.nextLocal)
      grouping.foreach {
        case (name, t) =>
          getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
            .invokeV("copy",
              dataModelVar.push().invokeV(name, t),
              groupingIterVar.push().invokeI("next", classOf[AnyRef].asType).cast(t))
      }

      val orderingIterVar = thisVar.push()
        .invokeV("ordering", classOf[Seq[_]].asType)
        .invokeI("iterator", classOf[Iterator[_]].asType).store(dataModelVar.nextLocal)
      ordering.foreach {
        case (name, t) =>
          getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
            .invokeV("copy",
              dataModelVar.push().invokeV(name, t),
              orderingIterVar.push().invokeI("next", classOf[AnyRef].asType).cast(t))
      }

      `return`()
    }
  }
}

object ShuffleKeyClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  private[this] val cache: mutable.Map[JPContext, mutable.Map[(String, Type, Seq[(String, Type)], Seq[(String, Type)]), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    flowId: String,
    dataModelType: Type,
    grouping: Seq[(String, Type)],
    ordering: Seq[(String, Type)])(implicit jpContext: JPContext): Type = {
    cache.getOrElseUpdate(jpContext, mutable.Map.empty).getOrElseUpdate(
      (flowId, dataModelType, grouping, ordering), {
        jpContext.addClass(new ShuffleKeyClassBuilder(flowId, dataModelType, grouping, ordering))
      })
  }
}

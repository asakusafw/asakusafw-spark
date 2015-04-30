package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.driver.OutputDriver
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class OutputDriverClassBuilder(
  val flowId: String,
  val dataModelType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/OutputDriver$$${OutputDriverClassBuilder.nextId};"),
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[OutputDriver[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
          }
        }
        .build(),
      classOf[OutputDriver[_]].asType) with DriverName {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[SparkContext].asType,
      classOf[Broadcast[Configuration]].asType,
      classOf[Seq[Future[RDD[(_, _)]]]].asType,
      classOf[mutable.Set[Future[Unit]]].asType),
      new MethodSignatureBuilder()
        .newParameterType(classOf[SparkContext].asType)
        .newParameterType {
          _.newClassType(classOf[Broadcast[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Configuration].asType)
          }
        }
        .newParameterType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Future[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[RDD[_]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                      _.newClassType(classOf[(_, _)].asType) {
                        _.newTypeArgument()
                          .newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
                      }
                    }
                  }
                }
              }
            }
          }
        }
        .newParameterType {
          _.newClassType(classOf[mutable.Set[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Future[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Unit].asType)
              }
            }
          }
        }
        .newVoidReturnType()
        .build()) { mb =>
        import mb._
        val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
        val hadoopConfVar = `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
        val prevsVar = `var`(classOf[Seq[Future[RDD[(_, _)]]]].asType, hadoopConfVar.nextLocal)
        val terminatorsVar = `var`(classOf[mutable.Set[Future[Unit]]].asType, prevsVar.nextLocal)

        thisVar.push().invokeInit(
          superType,
          scVar.push(),
          hadoopConfVar.push(),
          prevsVar.push(),
          terminatorsVar.push(),
          getStatic(ClassTag.getClass.asType, "MODULE$", ClassTag.getClass.asType)
            .invokeV("apply", classOf[ClassTag[_]].asType, ldc(dataModelType).asType(classOf[Class[_]].asType)))
      }
  }
}

object OutputDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}

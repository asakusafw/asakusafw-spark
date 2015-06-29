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
package com.asakusafw.spark

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.runtime.value._
import com.asakusafw.spark.tools.asm._

import resource._

package object compiler {

  val GeneratedClassPackageInternalName = "com/asakusafw/generated/spark"

  implicit class AugmentedTypeDescription(val desc: TypeDescription) extends AnyVal {

    def asType: Type =
      desc match {
        case desc: BasicTypeDescription => desc.asType
        case desc: ClassDescription => desc.asType
        case desc: ArrayTypeDescription => desc.asType
      }
  }

  implicit class AugmentedBasicTypeDescription(val desc: BasicTypeDescription) extends AnyVal {

    def asType: Type = {
      import BasicTypeDescription.BasicTypeKind // scalastyle:ignore
      desc.getBasicTypeKind match {
        case BasicTypeKind.VOID => Type.VOID_TYPE
        case BasicTypeKind.INT => Type.INT_TYPE
        case BasicTypeKind.LONG => Type.LONG_TYPE
        case BasicTypeKind.FLOAT => Type.FLOAT_TYPE
        case BasicTypeKind.DOUBLE => Type.DOUBLE_TYPE
        case BasicTypeKind.SHORT => Type.SHORT_TYPE
        case BasicTypeKind.CHAR => Type.CHAR_TYPE
        case BasicTypeKind.BYTE => Type.BYTE_TYPE
        case BasicTypeKind.BOOLEAN => Type.BOOLEAN_TYPE
      }
    }
  }

  implicit class AugmentedClassDescription(val desc: ClassDescription) extends AnyVal {

    def asType: Type = Type.getObjectType(desc.getInternalName)
  }

  implicit class AugmentedArrayTypeDescription(val desc: ArrayTypeDescription) extends AnyVal {

    def asType: Type = Type.getObjectType(s"[${desc.getComponentType.asType.getDescriptor}")
  }

  implicit class AugmentedJobflowProcessorContext(val context: JobflowProcessor.Context) extends AnyVal {

    def addClass(builder: ClassBuilder): Type = {
      addClass(builder.thisType, builder.build())
    }

    def addClass(t: Type, bytes: Array[Byte]): Type = {
      for {
        os <- managed(context.addClassFile(new ClassDescription(t.getClassName)))
      } {
        os.write(bytes)
      }
      t
    }
  }
}

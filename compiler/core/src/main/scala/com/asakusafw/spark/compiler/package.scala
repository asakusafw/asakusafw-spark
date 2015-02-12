package com.asakusafw.spark

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.description._

package object compiler {

  type MethodDesc = (String, Type)

  implicit class AugmentedTypeDescription(val desc: TypeDescription) extends AnyVal {

    def asType: Type =
      desc match {
        case desc: BasicTypeDescription => desc.asType
        case desc: ClassDescription     => desc.asType
        case desc: ArrayTypeDescription => desc.asType
      }
  }

  implicit class AugmentedBasicTypeDescription(val desc: BasicTypeDescription) extends AnyVal {

    def asType: Type = {
      import BasicTypeDescription.BasicTypeKind
      desc.getBasicTypeKind match {
        case BasicTypeKind.VOID    => Type.VOID_TYPE
        case BasicTypeKind.INT     => Type.INT_TYPE
        case BasicTypeKind.LONG    => Type.LONG_TYPE
        case BasicTypeKind.FLOAT   => Type.FLOAT_TYPE
        case BasicTypeKind.DOUBLE  => Type.DOUBLE_TYPE
        case BasicTypeKind.SHORT   => Type.SHORT_TYPE
        case BasicTypeKind.CHAR    => Type.CHAR_TYPE
        case BasicTypeKind.BYTE    => Type.BYTE_TYPE
        case BasicTypeKind.BOOLEAN => Type.BOOLEAN_TYPE
      }
    }
  }

  implicit class AugmentedClassDescription(val desc: ClassDescription) extends AnyVal {

    def asType: Type = Type.getType(s"L${desc.getName.replaceAll("\\.", "/")};")
  }

  implicit class AugmentedArrayTypeDescription(val desc: ArrayTypeDescription) extends AnyVal {

    def asType: Type = Type.getType(s"[${desc.getComponentType.asType.getDescriptor}")
  }
}

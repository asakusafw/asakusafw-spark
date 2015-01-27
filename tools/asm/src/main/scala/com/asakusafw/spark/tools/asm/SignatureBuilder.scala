package com.asakusafw.spark.tools.asm

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor
import org.objectweb.asm.signature.SignatureWriter

sealed abstract class SignatureBuilder(protected val sv: SignatureVisitor) {

  def build(): String = sv.toString()

  override def toString(): String = build()
}

final class ClassSignatureBuilder(sv: SignatureVisitor) extends SignatureBuilder(sv) {

  def this() = this(new SignatureWriter())

  private var visitedSuperclass = false
  private var visitedInterface = false

  def newFormalTypeParameter(name: String, classBound: Type, interfaceBounds: Type*): this.type = {
    newFormalTypeParameter(name) {
      _
        .newClassBound(classBound)
        .newInterfaceBounds(interfaceBounds: _*)
    }
  }

  def newFormalTypeParameter(name: String)(block: FormalTypeSignatureBuilder => Unit): this.type = {
    assert(!visitedSuperclass && !visitedInterface)
    sv.visitFormalTypeParameter(name)
    block(new FormalTypeSignatureBuilder(sv))
    this
  }

  def newSuperclass(`type`: Type): this.type = {
    newSuperclass {
      _.newClassType(`type`)
    }
  }

  def newSuperclass(block: TypeSignatureBuilder => Unit): this.type = {
    assert(!visitedSuperclass && !visitedInterface)
    sv.visitSuperclass()
    block(new TypeSignatureBuilder(sv))
    visitedSuperclass = true
    this
  }

  def newInterface(`type`: Type): this.type = {
    newInterface {
      _.newClassType(`type`)
    }
  }

  def newInterface(block: TypeSignatureBuilder => Unit): this.type = {
    sv.visitInterface()
    block(new TypeSignatureBuilder(sv))
    visitedInterface = true
    this
  }

  def newInterfaces(types: Type*): this.type = {
    types.foreach(newInterface)
    this
  }

  override def build(): String = {
    assert(visitedSuperclass)
    super.build()
  }
}

final class TypeSignatureBuilder(sv: SignatureVisitor) extends SignatureBuilder(sv) {

  def this() = this(new SignatureWriter())

  private var typed = false
  private var classType = false

  def newTypeVariable(name: String): this.type = {
    assert(!typed)
    sv.visitTypeVariable(name)
    typed = true
    this
  }

  def newArrayType(block: TypeSignatureBuilder => Unit): this.type = {
    assert(!typed)
    sv.visitArrayType()
    block(new TypeSignatureBuilder(sv))
    typed = true
    this
  }

  def newClassType(`type`: Type)(implicit block: TypeArgumentSignatureBuilder => Unit): this.type = {
    assert(!typed)
    if (`type`.getSort() < Type.ARRAY) {
      sv.visitBaseType(`type`.getDescriptor().charAt(0))
    } else if (`type`.getSort() == Type.ARRAY) {
      (0 until `type`.getDimensions()).foreach(_ => sv.visitArrayType())
      newClassType(`type`.getElementType())(block)
    } else {
      sv.visitClassType(`type`.getInternalName())
      block(new TypeArgumentSignatureBuilder(sv))
      sv.visitEnd()
    }
    typed = true
    classType = true
    this
  }

  def newInnnerClassType(`type`: Type)(implicit block: TypeArgumentSignatureBuilder => Unit): this.type = {
    assert(classType)
    sv.visitInnerClassType(`type`.getInternalName())
    block(new TypeArgumentSignatureBuilder(sv))
    sv.visitEnd()
    this
  }
}

object TypeSignatureBuilder {
  implicit val emptyTypeArgumentSignatureBuilderBlock: TypeArgumentSignatureBuilder => Unit = { sb => }
}

final class MethodSignatureBuilder(sv: SignatureVisitor) extends SignatureBuilder(sv) {

  def this() = this(new SignatureWriter())

  private var visitedParameterType = false
  private var visitedReturnType = false

  def newFormalTypeParameter(name: String, classBound: Type, interfaceBounds: Type*): this.type = {
    newFormalTypeParameter(name) {
      _
        .newClassBound(classBound)
        .newInterfaceBounds(interfaceBounds: _*)
    }
  }

  def newFormalTypeParameter(name: String)(block: FormalTypeSignatureBuilder => Unit): this.type = {
    assert(!visitedParameterType && !visitedReturnType)
    sv.visitFormalTypeParameter(name)
    block(new FormalTypeSignatureBuilder(sv))
    this
  }

  def newParameterType(`type`: Type): this.type = {
    newParameterType {
      _.newClassType(`type`)
    }
  }

  def newParameterType(block: TypeSignatureBuilder => Unit): this.type = {
    assert(!visitedReturnType)
    sv.visitParameterType()
    block(new TypeSignatureBuilder(sv))
    visitedParameterType = true
    this
  }

  def newParameterTypes(types: Type*): this.type = {
    types.foreach(newParameterType)
    this
  }

  def newVoidReturnType(): this.type = {
    newReturnType(Type.VOID_TYPE)
  }

  def newReturnType(`type`: Type): this.type = {
    newReturnType {
      _.newClassType(`type`)
    }
  }

  def newReturnType(block: TypeSignatureBuilder => Unit): this.type = {
    sv.visitReturnType()
    block(new TypeSignatureBuilder(sv))
    visitedReturnType = true
    this
  }

  def newExceptionType(`type`: Type): this.type = {
    newExceptionType {
      _.newClassType(`type`)
    }
  }

  def newExceptionType(block: TypeSignatureBuilder => Unit): this.type = {
    assert(visitedReturnType)
    sv.visitExceptionType()
    block(new TypeSignatureBuilder(sv))
    this
  }

  def newExceptionTypes(types: Type*): this.type = {
    types.foreach(newExceptionType)
    this
  }

  override def build(): String = {
    assert(visitedReturnType)
    super.build()
  }
}

final class FormalTypeSignatureBuilder private[asm] (sv: SignatureVisitor)
    extends SignatureBuilder(sv) {

  private var visitedClassBound = false
  private var visitedInterfaceBound = false

  def newClassBound(`type`: Type): this.type = {
    newClassBound {
      _.newClassType(`type`.boxed)
    }
  }

  def newClassBound(block: TypeSignatureBuilder => Unit): this.type = {
    assert(!visitedClassBound && !visitedInterfaceBound)
    sv.visitClassBound()
    block(new TypeSignatureBuilder(sv))
    visitedClassBound = true
    this
  }

  def newInterfaceBound(`type`: Type): this.type = {
    newInterfaceBound {
      _.newClassType(`type`.boxed)
    }
  }

  def newInterfaceBound(block: TypeSignatureBuilder => Unit): this.type = {
    sv.visitInterfaceBound()
    block(new TypeSignatureBuilder(sv))
    visitedInterfaceBound = true
    this
  }

  def newInterfaceBounds(types: Type*): this.type = {
    types.foreach(newInterfaceBound)
    this
  }
}

final class TypeArgumentSignatureBuilder private[asm] (sv: SignatureVisitor)
    extends SignatureBuilder(sv) {

  def newTypeArgument(block: TypeSignatureBuilder => Unit): this.type = {
    sv.visitTypeArgument()
    block(new TypeSignatureBuilder(sv))
    this
  }

  def newTypeArgument(wildcard: Char, `type`: Type): this.type = {
    newTypeArgument(wildcard) {
      _.newClassType(`type`.boxed)
    }
  }

  def newTypeArgument(wildcard: Char)(block: TypeSignatureBuilder => Unit): this.type = {
    sv.visitTypeArgument(wildcard)
    block(new TypeSignatureBuilder(sv))
    this
  }
}

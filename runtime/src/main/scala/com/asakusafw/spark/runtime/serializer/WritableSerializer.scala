package com.asakusafw.spark.runtime.serializer

import java.io.{ DataInputStream, DataOutputStream }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._

import com.asakusafw.runtime.model._
import com.asakusafw.runtime.value._

abstract class WritableSerializer[W <: Writable] extends Serializer[W](false, false) {

  private[this] val outputs = new ThreadLocal[(Output, DataOutputStream)]

  override def write(kryo: Kryo, output: Output, obj: W) = {
    val dos = outputs.get match {
      case (lastOutput, dos) if output == lastOutput =>
        dos
      case _ =>
        val dos = new DataOutputStream(output)
        outputs.set((output, dos))
        dos
    }
    try {
      obj.write(dos)
    } finally {
      dos.flush()
    }
  }

  private[this] val inputs = new ThreadLocal[(Input, DataInputStream)]

  override def read(kryo: Kryo, input: Input, t: Class[W]): W = {
    val dis = inputs.get match {
      case (lastInput, dis) if input == lastInput =>
        dis
      case _ =>
        val dis = new DataInputStream(input)
        inputs.set((input, dis))
        dis
    }
    val obj = newInstance()
    obj.readFields(dis)
    obj
  }

  def newInstance(): W
}

object WritableSerializer {

  trait ConfigurationSerializer extends WritableSerializer[Configuration] {
    override def newInstance(): Configuration = new Configuration()
  }
  object ConfigurationSerializer extends ConfigurationSerializer

  trait BooleanOptionSerializer extends WritableSerializer[BooleanOption] {
    override def newInstance(): BooleanOption = new BooleanOption()
  }
  object BooleanOptionSerializer extends BooleanOptionSerializer

  trait ByteOptionSerializer extends WritableSerializer[ByteOption] {
    override def newInstance(): ByteOption = new ByteOption()
  }
  object ByteOptionSerializer extends ByteOptionSerializer

  trait ShortOptionSerializer extends WritableSerializer[ShortOption] {
    override def newInstance(): ShortOption = new ShortOption()
  }
  object ShortOptionSerializer extends ShortOptionSerializer

  trait IntOptionSerializer extends WritableSerializer[IntOption] {
    override def newInstance(): IntOption = new IntOption()
  }
  object IntOptionSerializer extends IntOptionSerializer

  trait LongOptionSerializer extends WritableSerializer[LongOption] {
    override def newInstance(): LongOption = new LongOption()
  }
  object LongOptionSerializer extends LongOptionSerializer

  trait FloatOptionSerializer extends WritableSerializer[FloatOption] {
    override def newInstance(): FloatOption = new FloatOption()
  }
  object FloatOptionSerializer extends FloatOptionSerializer

  trait DoubleOptionSerializer extends WritableSerializer[DoubleOption] {
    override def newInstance(): DoubleOption = new DoubleOption()
  }
  object DoubleOptionSerializer extends DoubleOptionSerializer

  trait DecimalOptionSerializer extends WritableSerializer[DecimalOption] {
    override def newInstance(): DecimalOption = new DecimalOption()
  }
  object DecimalOptionSerializer extends DecimalOptionSerializer

  trait StringOptionSerializer extends WritableSerializer[StringOption] {
    override def newInstance(): StringOption = new StringOption()
  }
  object StringOptionSerializer extends StringOptionSerializer

  trait DateOptionSerializer extends WritableSerializer[DateOption] {
    override def newInstance(): DateOption = new DateOption()
  }
  object DateOptionSerializer extends DateOptionSerializer

  trait DateTimeOptionSerializer extends WritableSerializer[DateTimeOption] {
    override def newInstance(): DateTimeOption = new DateTimeOption()
  }
  object DateTimeOptionSerializer extends DateTimeOptionSerializer
}

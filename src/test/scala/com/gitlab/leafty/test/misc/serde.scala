package com.gitlab.leafty.test.misc

object serde {

    import com.esotericsoftware.kryo.{Kryo, Serializer}
    import com.esotericsoftware.kryo.io.{Input, Output}
    import org.apache.spark.serializer.KryoRegistrator

    class TrnSerializer extends Serializer[domain.Trn]{

        // #todo there must be a better way!
        def ser(kryo: Kryo) : Serializer[domain.Trn] = kryo.getDefaultSerializer(classOf[domain.Trn]).asInstanceOf[Serializer[domain.Trn]]

        override def write(kryo: Kryo, output: Output, a: domain.Trn): Unit = {
          ser(kryo).write(kryo, output, a)
        }

        override def read(kryo: Kryo, input: Input, t: Class[domain.Trn]): domain.Trn = {
          val trn : domain.Trn = ser(kryo).read(kryo, input, t)
          trn.copy(amount = trn.amount.setScale(2))
        }
    }

    class CustomRegistrator extends KryoRegistrator {

      override def registerClasses(kryo: Kryo) {
        kryo.register(classOf[domain.Trn], new TrnSerializer())
        Array(
          classOf[java.math.BigDecimal],
          classOf[org.apache.spark.sql.Row],
          classOf[org.apache.spark.sql.catalyst.expressions.GenericRow],
          classOf[Array[org.apache.spark.sql.Row]],
          //classOf[domain.Trn],
          classOf[Array[domain.Trn]],
          classOf[domain.Range]
        ).map(kryo.register)
      }
    }

}

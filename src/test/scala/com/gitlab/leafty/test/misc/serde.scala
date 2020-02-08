package com.gitlab.leafty.test.misc

import java.sql.Timestamp

import org.apache.spark.serializer.KryoRegistrator


object serde {

    import com.esotericsoftware.kryo.Kryo
    import com.esotericsoftware.kryo.io.{Input, Output}

    import domain.Trn._

    class TrnSerializer extends com.esotericsoftware.kryo.Serializer[domain.Trn]{
        override def write(kryo: Kryo, output: Output, a: domain.Trn): Unit = {
          //super.write(kryo, output, a)
          output.writeString(a.ctgId)
          output.writeString(a.amount.toString())
          output.writeLong(a.time.getTime)
        }

        override def read(kryo: Kryo, input: Input, t: Class[domain.Trn]): domain.Trn = {
          domain.Trn(input.readString(), amount(input.readString()), new Timestamp(input.readLong()))
        }
    }

    class CustomRegistrator extends KryoRegistrator {

      override def registerClasses(kryo: Kryo) {
        kryo.register(classOf[domain.Trn], new TrnSerializer())
        Array(
          classOf[java.math.BigDecimal],
          //classOf[domain.Trn],
          classOf[Array[domain.Trn]],
          classOf[domain.Range],
          classOf[org.apache.spark.sql.Row],
          classOf[org.apache.spark.sql.catalyst.expressions.GenericRow],
          classOf[Array[org.apache.spark.sql.Row]],
          Class.forName(
            "org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage")
        ).map(kryo.register)
      }
    }

}

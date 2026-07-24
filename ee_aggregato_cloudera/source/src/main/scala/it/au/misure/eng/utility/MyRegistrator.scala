package it.au.misure.eng.utility

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import java.nio.charset.Charset

import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(Charset.forName("UTF-8").getClass, KryoCharsetSeralizer.getCharsetCustomListSerializer)
    kryo.register(Charset.forName("UTF-16").getClass, KryoCharsetSeralizer.getCharsetCustomListSerializer)
    kryo.register(Charset.forName("UTF-16BE").getClass, KryoCharsetSeralizer.getCharsetCustomListSerializer)
    kryo.register(Charset.forName("UTF-16LE").getClass, KryoCharsetSeralizer.getCharsetCustomListSerializer)
    kryo.register(Charset.forName("ISO_8859_1").getClass, KryoCharsetSeralizer.getCharsetCustomListSerializer)
   // kryo.register(Charset.forName("US_ASCII").getClass, KryoCharsetSeralizer.getCharsetCustomListSerializer)
  }
}

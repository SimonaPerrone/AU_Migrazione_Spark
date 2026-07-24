package it.eng.au.deleteOldPartition.utility.kryo

import com.esotericsoftware.kryo.Kryo
import de.javakaffee.kryoserializers.jodatime.{JodaDateTimeSerializer, JodaLocalDateSerializer, JodaLocalDateTimeSerializer, JodaLocalTimeSerializer}
import org.apache.spark.serializer.KryoRegistrator
import org.joda.time.{DateTime, LocalDate, LocalDateTime}

class AggKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[DateTime], new JodaDateTimeSerializer())
    kryo.register(classOf[LocalDate], new JodaLocalDateSerializer())
    kryo.register(classOf[LocalDateTime], new JodaLocalDateTimeSerializer())
    kryo.register(classOf[LocalDateTime], new JodaLocalTimeSerializer())
  }
}

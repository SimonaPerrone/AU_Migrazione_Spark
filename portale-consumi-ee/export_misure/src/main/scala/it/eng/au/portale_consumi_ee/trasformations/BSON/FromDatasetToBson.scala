package it.eng.au.portale_consumi_ee.trasformations.BSON
import it.eng.au.portale_consumi_ee.model.Forniture.MisureElettricheModel
import it.eng.au.portale_consumi_ee.model.misure.{AutoletturaValues, VoltureValues, misureMensiliCStructValues, misureNonOrarieCStructValues, misureOrarieCStructValues}
import it.eng.au.portale_consumi_ee.schema.misure.{autolettureSchema, misureMensiliCToMongoDBSchema, misureNonOrarieCtoMongoDBSchema, misureOrarieCMongoDBSchema, voltureToMongoDBSchema}
import org.bson.{BsonArray, BsonDocument, BsonNull, BsonValue}
import org.mongodb.scala.bson.{BsonInt32, BsonString}


object FromDatasetToBson {

  def getBsonValue(strval: String): BsonValue = {
    if (strval == null || strval.trim.isEmpty) BsonNull.VALUE
    else new BsonString(strval)
  }


  def misureOrarieToBson(m: misureOrarieCStructValues): BsonDocument = {
    val doc = new BsonDocument()

    doc.put(misureOrarieCMongoDBSchema.giorno, getBsonValue(m.giorno))
    doc.put(misureOrarieCMongoDBSchema.competenza_consumi, getBsonValue(m.competenza_consumi))
    doc.put(misureOrarieCMongoDBSchema.consumo_giornaliero_gg, getBsonValue(m.consumo_giornaliero_gg))
    doc.put(misureOrarieCMongoDBSchema.lettura_misura_f1, getBsonValue(m.lettura_misura_f1))
    doc.put(misureOrarieCMongoDBSchema.lettura_misura_f2, getBsonValue(m.lettura_misura_f2))
    doc.put(misureOrarieCMongoDBSchema.lettura_misura_f3, getBsonValue(m.lettura_misura_f3))
    doc.put(misureOrarieCMongoDBSchema.lettura_misura_f4, getBsonValue(m.lettura_misura_f4))
    doc.put(misureOrarieCMongoDBSchema.lettura_misura_f5, getBsonValue(m.lettura_misura_f5))
    doc.put(misureOrarieCMongoDBSchema.lettura_misura_f6, getBsonValue(m.lettura_misura_f6))
    doc.put(misureOrarieCMongoDBSchema.delta_misure_f1, getBsonValue(m.delta_misure_f1))
    doc.put(misureOrarieCMongoDBSchema.delta_misure_f2, getBsonValue(m.delta_misure_f2))
    doc.put(misureOrarieCMongoDBSchema.delta_misure_f3, getBsonValue(m.delta_misure_f3))
    doc.put(misureOrarieCMongoDBSchema.delta_misure_f4, getBsonValue(m.delta_misure_f4))
    doc.put(misureOrarieCMongoDBSchema.delta_misure_f5, getBsonValue(m.delta_misure_f5))
    doc.put(misureOrarieCMongoDBSchema.delta_misure_f6, getBsonValue(m.delta_misure_f6))
    doc.put(misureOrarieCMongoDBSchema.potenza_max_erogata, getBsonValue(m.potenza_max_erogata))
    doc.put(misureOrarieCMongoDBSchema.tipo_misura, getBsonValue(m.tipo_misura))
    doc.put(misureOrarieCMongoDBSchema.data_lettura, getBsonValue(m.data_lettura))

    doc
  }

  def misureNonOrarieToBson(m: misureNonOrarieCStructValues): BsonDocument = {
    val doc = new BsonDocument()

    doc.put(misureNonOrarieCtoMongoDBSchema.competenza_consumi, getBsonValue(m.competenza_consumi))
    doc.put(misureNonOrarieCtoMongoDBSchema.delta_misure_monoraria, getBsonValue(m.delta_misure_monoraria))
    doc.put(misureNonOrarieCtoMongoDBSchema.lettura_misura_monoraria, getBsonValue(m.lettura_misura_monoraria))
    doc.put(misureNonOrarieCtoMongoDBSchema.lettura_misura_f1, getBsonValue(m.lettura_misura_f1))
    doc.put(misureNonOrarieCtoMongoDBSchema.lettura_misura_f2, getBsonValue(m.lettura_misura_f2))
    doc.put(misureNonOrarieCtoMongoDBSchema.lettura_misura_f3, getBsonValue(m.lettura_misura_f3))
    doc.put(misureNonOrarieCtoMongoDBSchema.lettura_misura_f4, getBsonValue(m.lettura_misura_f4))
    doc.put(misureNonOrarieCtoMongoDBSchema.lettura_misura_f5, getBsonValue(m.lettura_misura_f5))
    doc.put(misureNonOrarieCtoMongoDBSchema.lettura_misura_f6, getBsonValue(m.lettura_misura_f6))
    doc.put(misureNonOrarieCtoMongoDBSchema.delta_misure_f1, getBsonValue(m.delta_misure_f1))
    doc.put(misureNonOrarieCtoMongoDBSchema.delta_misure_f2, getBsonValue(m.delta_misure_f2))
    doc.put(misureNonOrarieCtoMongoDBSchema.delta_misure_f3, getBsonValue(m.delta_misure_f3))
    doc.put(misureNonOrarieCtoMongoDBSchema.delta_misure_f4, getBsonValue(m.delta_misure_f4))
    doc.put(misureNonOrarieCtoMongoDBSchema.delta_misure_f5, getBsonValue(m.delta_misure_f5))
    doc.put(misureNonOrarieCtoMongoDBSchema.delta_misure_f6, getBsonValue(m.delta_misure_f6))
    doc.put(misureNonOrarieCtoMongoDBSchema.tipo_misura, getBsonValue(m.tipo_misura))
    doc.put(misureNonOrarieCtoMongoDBSchema.data_lettura, getBsonValue(m.data_lettura))
    doc.put(misureNonOrarieCtoMongoDBSchema.potf1, getBsonValue(m.potf1))
    doc.put(misureNonOrarieCtoMongoDBSchema.potf2, getBsonValue(m.potf2))
    doc.put(misureNonOrarieCtoMongoDBSchema.potf3, getBsonValue(m.potf3))
    doc.put(misureNonOrarieCtoMongoDBSchema.potm, getBsonValue(m.potm))

    doc
  }

  def voltureToBson(v: VoltureValues): BsonDocument = {
    val doc = new BsonDocument()

    doc.put(voltureToMongoDBSchema.competenza_consumi, getBsonValue(v.competenza_consumi))
    doc.put(voltureToMongoDBSchema.data_lettura, getBsonValue(v.data_lettura))
    doc.put(voltureToMongoDBSchema.lettura_misura_monoraria, getBsonValue(v.lettura_misura_monoraria))
    doc.put(voltureToMongoDBSchema.lettura_misura_f1, getBsonValue(v.lettura_misura_f1))
    doc.put(voltureToMongoDBSchema.lettura_misura_f3, getBsonValue(v.lettura_misura_f3))
    doc.put(voltureToMongoDBSchema.lettura_misura_f4, getBsonValue(v.lettura_misura_f4))
    doc.put(voltureToMongoDBSchema.lettura_misura_f5, getBsonValue(v.lettura_misura_f5))
    doc.put(voltureToMongoDBSchema.lettura_misura_f6, getBsonValue(v.lettura_misura_f6))
    doc.put(voltureToMongoDBSchema.tipo_misura, getBsonValue(v.tipo_misura))

    doc
  }

  def autoletturaToBson(a: AutoletturaValues): BsonDocument = {
    val doc = new BsonDocument()

    doc.put(autolettureSchema.competenza_consumi, getBsonValue(a.competenza_consumi))
    doc.put(autolettureSchema.data_lettura, getBsonValue(a.data_lettura))
    doc.put(autolettureSchema.lettura_misura_monoraria, getBsonValue(a.lettura_misura_monoraria))
    doc.put(autolettureSchema.lettura_misura_f1, getBsonValue(a.lettura_misura_f1))
    doc.put(autolettureSchema.lettura_misura_f2, getBsonValue(a.lettura_misura_f2))
    doc.put(autolettureSchema.lettura_misura_f3, getBsonValue(a.lettura_misura_f3))
    doc.put(autolettureSchema.lettura_misura_f4, getBsonValue(a.lettura_misura_f4))
    doc.put(autolettureSchema.lettura_misura_f5, getBsonValue(a.lettura_misura_f5))
    doc.put(autolettureSchema.lettura_misura_f6, getBsonValue(a.lettura_misura_f6))

    doc
  }

  def misuremensiliToBson(m: misureMensiliCStructValues): BsonDocument = {
    val doc = new BsonDocument()

    doc.put(misureMensiliCToMongoDBSchema.competenza_consumi, getBsonValue(m.competenza_consumi))
    doc.put(misureMensiliCToMongoDBSchema.delta_misure_monoraria, getBsonValue(m.delta_misure_monoraria))
    doc.put(misureMensiliCToMongoDBSchema.lettura_misura_monoraria, getBsonValue(m.lettura_misura_monoraria))

    doc.put(misureMensiliCToMongoDBSchema.lettura_misura_f1, getBsonValue(m.lettura_misura_f1))
    doc.put(misureMensiliCToMongoDBSchema.lettura_misura_f2, getBsonValue(m.lettura_misura_f2))
    doc.put(misureMensiliCToMongoDBSchema.lettura_misura_f3, getBsonValue(m.lettura_misura_f3))
    doc.put(misureMensiliCToMongoDBSchema.lettura_misura_f4, getBsonValue(m.lettura_misura_f4))
    doc.put(misureMensiliCToMongoDBSchema.lettura_misura_f5, getBsonValue(m.lettura_misura_f5))
    doc.put(misureMensiliCToMongoDBSchema.lettura_misura_f6, getBsonValue(m.lettura_misura_f6))

    doc.put(misureMensiliCToMongoDBSchema.delta_misure_f1, getBsonValue(m.delta_misure_f1))
    doc.put(misureMensiliCToMongoDBSchema.delta_misure_f2, getBsonValue(m.delta_misure_f2))
    doc.put(misureMensiliCToMongoDBSchema.delta_misure_f3, getBsonValue(m.delta_misure_f3))
    doc.put(misureMensiliCToMongoDBSchema.delta_misure_f4, getBsonValue(m.delta_misure_f4))
    doc.put(misureMensiliCToMongoDBSchema.delta_misure_f5, getBsonValue(m.delta_misure_f5))
    doc.put(misureMensiliCToMongoDBSchema.delta_misure_f6, getBsonValue(m.delta_misure_f6))

    doc.put(misureMensiliCToMongoDBSchema.tipo_misura, getBsonValue(m.tipo_misura))
    doc.put(misureMensiliCToMongoDBSchema.data_lettura, getBsonValue(m.data_lettura))

    doc.put(misureMensiliCToMongoDBSchema.potf1, getBsonValue(m.potf1))
    doc.put(misureMensiliCToMongoDBSchema.potf2, getBsonValue(m.potf2))
    doc.put(misureMensiliCToMongoDBSchema.potf3, getBsonValue(m.potf3))
    doc.put(misureMensiliCToMongoDBSchema.potm, getBsonValue(m.potm))

    doc
  }

  def misureElettricheToBson(m: MisureElettricheModel): BsonDocument = {
    val doc = new BsonDocument()

    doc.put("_id", BsonString(m._id))
    doc.put("codice_fornitura", BsonString(m.codice_fornitura))
    doc.put("competenza_consumi", BsonInt32(m.competenza_consumi))
    doc.put("pod",BsonString(m.pod))


    // Handle nested List[misureOrarieCStructValues]
    if (m.misure_orarie != null && m.misure_orarie.nonEmpty) {
      val misureOrarieArray = new BsonArray()
      m.misure_orarie.foreach { item =>
        misureOrarieArray.add(misureOrarieToBson(item))
      }
      doc.put("misure_orarie", misureOrarieArray)
    }

    // Handle nested misureMensiliCStructValues
    if (m.misure_mensili != null) doc.put("misure_mensili", misuremensiliToBson(m.misure_mensili))
    //else doc.put("misure_mensili", org.bson.BsonNull.VALUE)

    // Handle nested misureNonOrarieCStructValues
    if (m.misure_non_orarie != null) doc.put("misure_non_orarie", misureNonOrarieToBson(m.misure_non_orarie))
    if (m.misure_non_orarie != null && m.misure_mensili == null) doc.put("misure_mensili", misureNonOrarieToBson(m.misure_non_orarie))

    //else doc.put("misure_non_orarie", org.bson.BsonNull.VALUE)

    // Handle VoltureValues
    if (m.volture != null) doc.put("volture", voltureToBson(m.volture))
    //else doc.put("volture", org.bson.BsonNull.VALUE)

    // Handle AutoletturaValues
    if (m.autoletture != null) doc.put("autoletture", autoletturaToBson(m.autoletture))
    //else doc.put("autoletture", org.bson.BsonNull.VALUE)

    doc
  }



}

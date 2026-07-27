package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO._
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Swg1}
import it.eng.au.aggiustamentoGas.schema.measure.Swg1Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class Swg1DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getSw1ParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    Swg1Schema.cod_servizio,
    Swg1Schema.cod_flusso,
    Swg1Schema.cod_pdr,
    Swg1Schema.data_prest,
    Swg1Schema.let_tot_prel,
    Swg1Schema.let_tot_conv,
    Swg1Schema.segn_mis_sost,
    Swg1Schema.segn_conv,
    Swg1Schema.matr_mis,
    Swg1Schema.matr_conv,
    Swg1Schema.local_file,
    Swg1Schema.tipo_lettura,
    Swg1Schema.d_caricamento,
    Swg1Schema.piva_distr,
    Swg1Schema.piva_utente,
    Swg1Schema.data_deco_switch,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => {
    (f.ammissibilita.isDefined || f.asInstanceOf[Swg1].flowCode.getOrElse("-").equalsIgnoreCase("0350")) &&
      ((f.ammissibilita.isEmpty && Set('E', 'S').contains(f.readType.getOrElse('-'))) ||
        Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))
        )
  }
  override val mapFunc: Row => Flow = (r: Row) => {
    val dataPrest = r.getAs[String](Swg1Schema.data_prest)
    val dataDecoSwitch = r.getAs[String](Swg1Schema.data_deco_switch)
    val letTotPrel = r.getAs[String](Swg1Schema.let_tot_prel)
    val letTotConv = r.getAs[String](Swg1Schema.let_tot_conv)
    val segnMisSost = r.getAs[String](Swg1Schema.segn_mis_sost)
    val segnConv = r.getAs[String](Swg1Schema.segn_conv)

    Swg1(
      service = r.getAs[String](Swg1Schema.cod_servizio).toUpperCase,
      flowCode = Option(r.getAs[String](Swg1Schema.cod_flusso)),
      pdr = r.getAs(Swg1Schema.cod_pdr).toString,
      readType = Try(Option(r.getAs[String](Swg1Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(dataDecoSwitch, dataPrest),
      measure = MeasureDAO.getDoubleField(letTotPrel, segnMisSost),
      converted = MeasureDAO.getDoubleField(letTotConv, segnConv),
      serialNumberMis = Option(r.getAs[String](Swg1Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Swg1Schema.matr_conv)),
      localFile = Option(r.getAs[String](Swg1Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Swg1Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Swg1Schema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](Swg1Schema.d_caricamento), dateLoadFormatter)
    )
  }
}

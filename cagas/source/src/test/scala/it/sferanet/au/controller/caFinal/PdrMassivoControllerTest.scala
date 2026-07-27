package it.sferanet.au.controller.caFinal

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.schema._
import it.sferanet.au.utilities.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.junit.Assert

class PdrMassivoControllerTest extends EnvironmentSparkTest {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def testGetQuery(): Unit = {

    Environment.setProperty("contractContinuity.upperBound.data", "2021-05-01")

    log.error(Environment.getContractContuinityUpperBoundDate)

    val dummyPdrMassivoController = new DummyPdrMassivoController
    val pdrMassivo = dummyPdrMassivoController.getPdrMassivoFull.persist(StorageLevel.MEMORY_AND_DISK)
    pdrMassivo.explain(true)

    Assert.assertNotEquals(0, pdrMassivo.count())

    Assert.assertEquals(0,
      pdrMassivo.where(col(RcuGasMassivoCaPSchema.t_codice_pdr).isNull.or(col(RcuGasMassivoCaPSchema.t_codice_pdr) === "")).count()
    )
    Assert.assertEquals(0,
      pdrMassivo.where(col(RcuGasMassivoCaPSchema.n_id_pdr).isNull.or(col(RcuGasMassivoCaPSchema.n_id_pdr) === "")).count()
    )
    Assert.assertEquals(0,
      pdrMassivo.where(col(PrtIstatRegioneClimaticaPSchema.t_regione_climatica) =!= "A").count()
    )
    Assert.assertEquals(4,
      pdrMassivo.where(col(PrtIstatRegioneClimaticaPSchema.t_regione_climatica) === "A").count()
    )

    pdrMassivo.show(false)

    val pdrMassivoFinal = dummyPdrMassivoController.get().cache()
    pdrMassivoFinal.show(false)

    Assert.assertEquals(pdrMassivo.count(),
      pdrMassivoFinal
        .where(col(PdrMassivoSchema.tipo_trasmissione) === lit(Environment.getTipoTrasmissione))
        .count()
    )
    Assert.assertEquals(1,
      pdrMassivoFinal.where(col(PdrMassivoSchema.codice_pdr) === "05260000050555").count()
    )
  }

  class DummyPdrMassivoController extends PdrMassivoController {

    override def getRcuGasMassivoCaP: DataFrame = {
      val sqlctx = Environment.getSqlContext
      import sqlctx.implicits._
      List(
        ("150604000000000169", "05260000050547", "n_id_az_udd", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "2020", "A"),
        ("150604000000000170", "05260000050570", "n_id_az_udd", "2030-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "2020", "A"),
        ("150604000000000171", "05260000050571", "n_id_az_udd", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "2020", "A"),
        ("150604000000000172", "05260000050572", "n_id_az_udd", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "2020", "A"),
        ("150604000000000149", "05260000050549", "n_id_az_udd", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "2019", "A"),
        ("150604000000000149", "05260000050549", "n_id_az_udd", "2018-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "2020", "A"),
        ("150604000000000166", "05260000050555", "n_id_az_udd", "1998-11-21 00:00:00.0", "2021-05-01 00:00:00.0", "016024", "2020", "A")
      ).toDF(
        RcuGasMassivoCaPSchema.n_id_pdr,
        RcuGasMassivoCaPSchema.t_codice_pdr,
        RcuGasMassivoCaPSchema.n_id_az_udd,
        RcuGasMassivoCaPSchema.d_data_inizio_for,
        RcuGasMassivoCaPSchema.data_fine_for,
        RcuGasMassivoCaPSchema.t_comune_istat_pdr,
        RcuGasMassivoCaPSchema.t_anno_termico,
        RcuGasMassivoCaPSchema.id_regione_climatica
      )
    }

    override def getRcuGasVarProfiloP: DataFrame = {
      val sqlctx = Environment.getSqlContext
      import sqlctx.implicits._
      List(
        ("150604000000000169", "C3E1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000170", "C3E1", "2030-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000171", "C3E1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000172", "C3E1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000149", "C3E1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000149", "C3E1", "2018-11-21 00:00:00.0", "2050-11-21 00:00:00.0")
      ).toDF(
        RcuGasVarProfiloPSchema.n_id_pdr,
        RcuGasVarProfiloPSchema.t_cod_profilo,
        RcuGasVarProfiloPSchema.d_data_inizio,
        RcuGasVarProfiloPSchema.d_data_fine
      )
    }

    override def getRcugasVarTrattamentoP: DataFrame = {
      val sqlctx = Environment.getSqlContext
      import sqlctx.implicits._
      List(
        ("150604000000000169", "t_trattamento", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000170", "t_trattamento", "2030-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000171", "t_trattamento", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000172", "t_trattamento", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000149", "t_trattamento", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000149", "t_trattamento", "2018-11-21 00:00:00.0", "2050-11-21 00:00:00.0")
      ).toDF(
        RcuGasVarTrattamentoPSchema.n_id_pdr,
        RcuGasVarTrattamentoPSchema.t_trattamento_settlement,
        RcuGasVarTrattamentoPSchema.d_data_inizio,
        RcuGasVarTrattamentoPSchema.d_data_fine
      )
    }

    override def getRcuGasVarPrelAnnuoP: DataFrame = {
      val sqlctx = Environment.getSqlContext
      import sqlctx.implicits._
      List(
        ("150604000000000169", "10159", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000170", "10159", "2030-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000171", "10159", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000172", "10159", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000149", "10159", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
        ("150604000000000149", "10159", "2018-11-21 00:00:00.0", "2050-11-21 00:00:00.0")
      ).toDF(
        RcuGasVarPrelAnnuoPSchema.n_id_pdr,
        RcuGasVarPrelAnnuoPSchema.n_prelievo_annuo,
        RcuGasVarPrelAnnuoPSchema.d_data_inizio,
        RcuGasVarPrelAnnuoPSchema.d_data_fine
      )
    }

    override def getRcuGasConnessioniDistr2P: DataFrame = {
      // t_codice_pdr,n_id_pdr,n_id_remi,d_data_inizio_conn,d_data_fine_conn,t_remi,n_id_distr,d_data_inizio_gestecn,d_data_fine_gestecn,t_remi_rcu,id_regione_climatica
      val rows = Seq(
        Row("05260000050547", "150604000000000169", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
        Row("05260000050548", "150604000000000148", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
        Row("05260000050549", "150604000000000149", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
        Row("05260000050570", "150604000000000170", "", "2030-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
        Row("05260000050571", "150604000000000171", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
        Row("05260000050572", "150604000000000172", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
        Row("05260000050555", "150604000000000166", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1")
      )
      Environment.getSqlContext.createDataFrame(Environment.getSparkContext.parallelize(rows), RcuGasConnessioniDistr2PSchema.createSparkSchema())
    }

    override def getRcuGasBilanciamentoP: DataFrame = {
      // n_id_bilanciamento , n_id_udb , n_id_pdr , t_tipo_bilanciamento , d_data_inizio , d_data_fine , t_note , d_aggiornamento , n_id_traccia , n_id_s_prec , d_data_rif
      val rows = Seq(
        Row("aaa", "", "150604000000000169", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "")
      )
      Environment.getSqlContext.createDataFrame(Environment.getSparkContext.parallelize(rows), RcuGasBilanciamentoPSchema.createSparkSchema())
    }

    override def getPrtIstatRegioneClimaticaP: DataFrame = {
      //t_codice_istat, t_regione_climatica
      val rows = Seq(
        Row("016024", "A"),
        Row("016025", "B"),
        Row("016026", "C"),
        Row("016027", "D"),
        Row("016028", "E")
      )
      Environment.getSqlContext.createDataFrame(Environment.getSparkContext.parallelize(rows), PrtIstatRegioneClimaticaPSchema.createSparkSchema())
    }
  }
}

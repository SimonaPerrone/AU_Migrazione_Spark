package it.sferanet.au

import it.sferanet.au.model.Flow
import it.sferanet.au.utilities.{Constants, Environment}
import it.sferanet.au.utility.Checker
import org.apache.spark.rdd.RDD
import org.junit.{Assert, Test}

import java.text.SimpleDateFormat

class MeasureIntegrationTest extends EnvironmentSparkTest with Checker {
  //Environment.resetProperty()

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
  val dateFormat2: SimpleDateFormat = Constants.FORMAT_DATE_LOAD


  @Test
  def test(): Unit = {
    //Environment.resetProperty()

    Environment.setProperty("flow.read.startDate", "0")
    Environment.setProperty("flow.read.endDate", "202112")
    Environment.setProperty("flow.read.receive.endDate", "202112")

    val otherPropRootPath = "src/test/resources/AppTest/filterModePdr"
    val measureRootPath = "src/test/resources/integrazione-ca/misure"

    //SET TABLES PATH
    setOtherProp(otherPropRootPath)
    setParquetPathMeasure(measureRootPath)

    //RUN TEST AND VERIFY

    val measure = App.extractMeasures().cache()

    checkMeasure(measure, "A02R", "01613550008444")
    checkMeasure(measure, "S02", "10400000021899")
    checkMeasure(measure, "R40R", "61493488000452")
    checkMeasure(measure, "R40", "09951201424304")
    checkMeasure(measure, "TGL", "15350500800817")
    checkMeasure(measure, "TGL", "00600002006264")
    checkMeasure(measure, "TML", "00105200028333")
    checkMeasure(measure, "TML", "00105200028336")
    checkMeasure(measure, "TML", "11610000110402")
    checkMeasure(measure, "TML", "11610000118102")
    checkMeasure(measure, "TAL", "03130000814534")
    checkMeasure(measure, "TAL", "03081000238930")
    checkMeasure(measure, "TAV", "15340000007189")
    checkMeasure(measure, "TAV", "02260146510020")
    checkMeasure(measure, "TAS", "03340011173335")
    checkMeasure(measure, "TAS", "03340007467887")
    checkMeasure(measure, "TAS", "11370000037631")
    checkMeasure(measure, "TMV", "00352800186457")
    checkMeasure(measure, "TMV", "03390000023956")
    checkMeasure(measure, "SW1", "15270000000546")
    checkMeasure(measure, "RSL", "00594200434084")
    checkMeasure(measure, "RSL", "07780000005000")
    checkMeasure(measure, "RMV", "00600036001437")
    checkMeasure(measure, "RMV", "15330000042252")
    checkMeasure(measure, "RML", "03270033216381")
    checkMeasure(measure, "RML", "01023900023221")
    checkMeasure(measure, "RGL", "03050000152313")
    checkMeasure(measure, "RGL", "11750001080026")
    checkMeasure(measure, "A01", "08200000026362")
    checkMeasure(measure, "A01", "10400000116081")
    checkMeasure(measure, "A01R", "01611300108810")
    checkMeasure(measure, "A40", "01510000013225")
    checkMeasure(measure, "A40", "61491833004856")
    checkMeasure(measure, "SM1", "03050000064553")
    checkMeasure(measure, "SM1", "00882105631622")
    checkMeasure(measure, "IM1PRE", "05260200843562")
    checkMeasure(measure, "IM1POST", "05260200843562")
    checkMeasure(measure, "IGMGPRE", "03160000253928")
    checkMeasure(measure, "IGMGPOST", "03160000253928")
    checkMeasure(measure, "M01R", "11690000005341")
    checkMeasure(measure, "R01R", "01613955000157")
    checkMeasure(measure, "A02", "15964204320469")
    checkMeasure(measure, "A02", "00883201890187")
    checkMeasure(measure, "AD2", "05780000459177")
    checkMeasure(measure, "AD3", "10430000020301")
    checkMeasure(measure, "AD4", "01611836000444")
    checkMeasure(measure, "AD5", "16280000305431")
    //    checkMeasure(measure,"FDD","00108700079854")
    checkMeasure(measure, "FDD", "15810000024808")
    checkMeasure(measure, "FUI", "15810000024807")
    //    checkMeasure(measure,"FUI","05260200281588")
    checkMeasure(measure, "M01", "01611113001514")
    checkMeasure(measure, "R01", "11420000001608")
    checkMeasure(measure, "R01", "10930000016834")
    checkMeasure(measure, "S40", "00882600028944")
    //    checkMeasure(measure,"SWG1","08200000016470")
    checkMeasure(measure, "V01", "03270027702390")
    checkMeasure(measure, "V02", "10400000465642")
    checkMeasure(measure, "A40R", "03100000074105")
    checkMeasure(measure, "AD2R", "61960010766896")
    checkMeasure(measure, "AD3R", "10430000020301")
    checkMeasure(measure, "AD4R", "02660000462866")
    checkMeasure(measure, "AD5R", "00881906667948")
    checkMeasure(measure, "S02R", "00882608306987")
    checkMeasure(measure, "S40R", "09951201978044")
    checkMeasure(measure, "V01R", "11980100100759")
    checkMeasure(measure, "V02R", "02800000068420")
    checkMeasure(measure, "SM1R", "01611518004021")
    checkMeasure(measure, "SM1R", "09640000141565")
  }

  private def setParquetPathMeasure(measureRoot: String): Unit = {

    //SET TEST TABLES PATHS
    Environment.setProperty("flow.dataset.a02r.basePath", s"$measureRoot/a02r/new_route")
    Environment.setProperty("flow.dataset.s02.basePath", s"$measureRoot/s02")
    Environment.setProperty("flow.dataset.r40r.basePath", s"$measureRoot/r40r")
    Environment.setProperty("flow.dataset.r40.basePath", s"$measureRoot/r40")
    Environment.setProperty("flow.dataset.tgl.basePath", s"$measureRoot/tgl/both_route")
    Environment.setProperty("flow.dataset.tml.basePath", s"$measureRoot/tml/both_route")
    Environment.setProperty("flow.dataset.tal.basePath", s"$measureRoot/tal/both_route")
    Environment.setProperty("flow.dataset.tav.basePath", s"$measureRoot/tav/both_route")
    Environment.setProperty("flow.dataset.tas.basePath", s"$measureRoot/tas/both_route")
    Environment.setProperty("flow.dataset.tmv.basePath", s"$measureRoot/tmv/both_route")
    Environment.setProperty("flow.dataset.sw1.basePath", s"$measureRoot/sw1/old_and_standard")
    Environment.setProperty("flow.dataset.rsl.basePath", s"$measureRoot/rsl/both_route")
    Environment.setProperty("flow.dataset.rmv.basePath", s"$measureRoot/rmv/both_route")
    Environment.setProperty("flow.dataset.rml.basePath", s"$measureRoot/rml/both_route")
    Environment.setProperty("flow.dataset.rgl.basePath", s"$measureRoot/rgl/both_route")
    Environment.setProperty("flow.dataset.a01.basePath", s"$measureRoot/a01/both_route")
    Environment.setProperty("flow.dataset.a01r.basePath", s"$measureRoot/a01r/new_route")
    Environment.setProperty("flow.dataset.a40.basePath", s"$measureRoot/a40/both_route")
    Environment.setProperty("flow.dataset.sm1.basePath", s"$measureRoot/sm1/both_route")
    Environment.setProperty("flow.dataset.im1post.basePath", s"$measureRoot/im1post/old_route")
    Environment.setProperty("flow.dataset.im1pre.basePath", s"$measureRoot/im1pre/old_route")
    Environment.setProperty("flow.dataset.igmgpost.basePath", s"$measureRoot/igmgPost")
    Environment.setProperty("flow.dataset.igmgpre.basePath", s"$measureRoot/igmgPre")
    Environment.setProperty("flow.dataset.m01r.basePath", s"$measureRoot/m01r")
    Environment.setProperty("flow.dataset.r01r.basePath", s"$measureRoot/r01r")
    Environment.setProperty("flow.dataset.a02.basePath", s"$measureRoot/a02/both_route")
    Environment.setProperty("flow.dataset.ad2.basePath", s"$measureRoot/ad2/new_route")
    Environment.setProperty("flow.dataset.ad3.basePath", s"$measureRoot/ad3/new_route")
    Environment.setProperty("flow.dataset.ad4.basePath", s"$measureRoot/ad4/new_route")
    Environment.setProperty("flow.dataset.ad5.basePath", s"$measureRoot/ad5/new_route")
    Environment.setProperty("flow.dataset.fdd.basePath", s"$measureRoot/fdd/both_route")
    Environment.setProperty("flow.dataset.fui.basePath", s"$measureRoot/fui/new_route/eff_and_sost")
    Environment.setProperty("flow.dataset.m01.basePath", s"$measureRoot/m01/both_route")
    Environment.setProperty("flow.dataset.r01.basePath", s"$measureRoot/r01/both_route")
    Environment.setProperty("flow.dataset.s40.basePath", s"$measureRoot/s40")
    Environment.setProperty("flow.dataset.swg1.basePath", s"$measureRoot/swg1/standard_and_old")
    Environment.setProperty("flow.dataset.v01.basePath", s"$measureRoot/v01/both_route")
    Environment.setProperty("flow.dataset.v02.basePath", s"$measureRoot/v02/both_route")
    Environment.setProperty("flow.dataset.a40r.basePath", s"$measureRoot/a40r/new_route")
    Environment.setProperty("flow.dataset.ad2r.basePath", s"$measureRoot/ad2r/new_route")
    Environment.setProperty("flow.dataset.ad3r.basePath", s"$measureRoot/ad3r/new_route")
    Environment.setProperty("flow.dataset.ad4r.basePath", s"$measureRoot/ad4r/new_route")
    Environment.setProperty("flow.dataset.ad5r.basePath", s"$measureRoot/ad5r/new_route")
    Environment.setProperty("flow.dataset.s02r.basePath", s"$measureRoot/s02r")
    Environment.setProperty("flow.dataset.s40r.basePath", s"$measureRoot/s40r")
    Environment.setProperty("flow.dataset.v01r.basePath", s"$measureRoot/v01r")
    Environment.setProperty("flow.dataset.v02r.basePath", s"$measureRoot/v02r")
    Environment.setProperty("flow.dataset.sm1r.basePath", s"$measureRoot/sm1r/new_route")

  }

  private def setOtherProp(rootPath: String): Unit = {
    //SET TEST TABLES PATHS
    Environment.setProperty("rcugas.basepath", s"$rootPath/rcugas.db/rcugas_massivo_ca_p")
    Environment.setProperty("rcugas_massivo__p.basepath", s"$rootPath/RCUGAS.RCUGAS_MASSIVO_p")
    Environment.setProperty("rcugas_connessioni_distr2__p.basepath", s"$rootPath/RCUGAS.RCUGAS_CONNESSIONI_DISTR2_p")
    Environment.setProperty("rcugas_bilanciamento_p.basepath", s"$rootPath/RCUGAS.RCUGAS_BILANCIAMENTO_p")
    Environment.setProperty("istat_regione_climatica_p.basepath", s"$rootPath/prt.istat_regione_climatica_p")
    Environment.setProperty("rcugas_udb_p.basepath", s"$rootPath/rcugas.rcugas_udb_p")
    Environment.setProperty("rcu_azienda_p.basepath", s"$rootPath/rcu.rcu_azienda_p")
    Environment.setProperty("gas_tds.basepath", s"$rootPath/gas_tds_part")
    Environment.setProperty("ca_pre_final.basepath", s"$rootPath/ca_pre_final")
    Environment.setProperty("ca_final.basepath", s"$rootPath/ca_final")
    Environment.setProperty("ca.basePath", s"$rootPath/ca")
    Environment.setProperty("consumption.basepath", s"$rootPath/consumption")
    Environment.setProperty("validation.basepath", s"$rootPath/validation")
    Environment.setProperty("v_rcugas_distributore_p.basepath", s"$rootPath/v_rcugas_distributore_p")
  }

  private def checkMeasure(measures: RDD[Flow], service: String, pdr: String): Unit = {
    Assert.assertTrue(measures.filter(f => f.service == service).collect().toList.map(f => f.pdr).contains(pdr))
  }
}

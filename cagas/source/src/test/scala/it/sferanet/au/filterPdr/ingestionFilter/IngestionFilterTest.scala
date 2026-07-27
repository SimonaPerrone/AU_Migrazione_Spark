package it.sferanet.au.filterPdr.ingestionFilter

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.model.prestazionale.{A01, AD2, AD3, IgmgPost, IgmgPre}
import it.sferanet.au.model.rettifica.{Rgl, Rml}
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.junit.Assert

import java.text.SimpleDateFormat

class IngestionFilterTest extends EnvironmentSparkTest {
  def testRemoveDuplicates(): Unit = {
    Assert.assertTrue(IngestionFilter.isDuplicateFilterEnabled)

    val format = new SimpleDateFormat("yyyy-MM-dd")
    val italianFormatter = new SimpleDateFormat("dd/MM/yyyy")
    val date = format.parse("2020-01-01")

    val measures: RDD[Flow] = Environment.getSparkContext.parallelize(List(
      // Case 1: flows are grouped by pdr, service, date and localFile, but they have different values in measure field. Result => 0 flows
      Tgl(service = "TGL", pdr = "pdr", readType = None, date = Some(date), measure = Some(2.0), converted = None,
        serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = None, isValid = None, d_caricamento = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"),
        ammissibilita = None, isNewRoute = true),
      Tgl(service = "TGL", pdr = "pdr", readType = None, date = Some(date), measure = Some(1.0), converted = Some(2.0),
        serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva2"), serialNumberConv = None, isValid = None, d_caricamento = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"),
        ammissibilita = None, isNewRoute = true),
      Tgl(service = "TGL", pdr = "pdr", readType = None, date = Some(date), measure = Some(3.0), converted = None,
        serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = None, isValid = None, d_caricamento = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"),
        ammissibilita = None, isNewRoute = true),
      // Case 1: they are grouped by pdr, service, date, localFile (there are two groups, 1-2 and 3-4): all the groups have the same fields. Result => 4 flows
      // Case 2: the remaining flows are grouped by timestampLocalFile and fileName is checked. Result => 0 flows
      AD2(service = "AD2", pdr = "pdr1", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("01/01/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), readType = Some('S'),
        ammissibilita = None, raccolta = None, isNewRoute = true),
      AD2(service = "AD2", pdr = "pdr1", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("02/02/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), readType = Some('S'),
        ammissibilita = None, raccolta = None, isNewRoute = true),
      AD2(service = "AD2", pdr = "pdr1", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("03/03/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.XML"), readType = Some('S'),
        ammissibilita = None, raccolta = None, isNewRoute = true),
      AD2(service = "AD2", pdr = "pdr1", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("04/04/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.XML"), readType = Some('S'),
        ammissibilita = None, raccolta = None, isNewRoute = true),
      // Case 1: they are grouped by pdr, service, date, localFile (there are four groups, 1,2,3,4). Result => 4 flows
      // Case 2: the remaining flows are grouped by timestampLocalFile and fileName (.toUppercase) is checked. Result => 4 flows
      // Case 3: the remaining flows are grouped by fileName, and localFiles are checked: only first two files have the correct piva. Result => 2 flows
      // Case 4: the remaining flows are grouped by localFile, and everything but dCaricamento is checked. Then the most recent flow is chosen. Result => 2 flows
      AD3(service = "AD3", pdr = "pdr2", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("01/01/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_piva1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), readType = Some('S'),
        ammissibilita = None, raccolta = None, isNewRoute = true),
      AD3(service = "AD3", pdr = "pdr2", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("02/02/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_piva1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xMl"), readType = Some('S'),
        ammissibilita = None, raccolta = None, isNewRoute = true),
      AD3(service = "AD3", pdr = "pdr2", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("03/03/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.Xml"), readType = Some('S'),
        ammissibilita = None, raccolta = None, isNewRoute = true),
      AD3(service = "AD3", pdr = "pdr2", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("04/04/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.XML"), readType = Some('S'),
        ammissibilita = None, raccolta = None, isNewRoute = true),
      // Case 1: they are grouped by pdr, service, date, localFile (there are two groups, 1-2 and 3-4): all the groups have the same fields. Result => 4 flows
      // Case 2: the remaining flows are not passed to case 2 since it's A01 flow. Result => 4 flows
      // Case 3: the remaining flows are grouped by fileName, and localFiles are checked. Result => 4 flows
      // Case 4: the remaining flows are grouped by localFile, and everything but dCaricamento is checked. Then the most recent flow is chosen. Result => 2 flows
      A01(service = "A01", pdr = "pdr3", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("01/01/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), readType = Some('S'),
        outcome = None, collected = None, ammissibilita = None, isNewRoute = true),
      A01(service = "A01", pdr = "pdr3", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("02/02/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), readType = Some('S'),
        outcome = None, collected = None, ammissibilita = None, isNewRoute = true),
      A01(service = "A01", pdr = "pdr3", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("03/03/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), readType = Some('S'),
        outcome = None, collected = None, ammissibilita = None, isNewRoute = true),
      A01(service = "A01", pdr = "pdr3", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("04/04/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), readType = Some('S'),
        outcome = None, collected = None, ammissibilita = None, isNewRoute = true),
      // Case 1: they are grouped by pdr, service, date, localFile (there are two groups, 1-2 and 3-4): the first group is discarded since motivation is different. Result => 2 flows
      // Case 2: the remaining flows are grouped by timestampLocalFile and fileName is checked. Result => 2 flows
      // Case 3: the remaining flows are grouped by fileName, and localFiles are checked. Result => 2 flows
      // Case 4: the remaining flows are grouped by localFile, and everything but dCaricamento is checked. Then the most recent flow is chosen. Result => 1 flow (the last one)
      Rgl(service = "RGL", pdr = "pdr4", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("01/01/2021")), motivation = Some(5), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"),
        collected = None, ammissibilita = None, isNewRoute = true),
      Rgl(service = "RGL", pdr = "pdr4", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("02/02/2021")), motivation = Some(4), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"),
        collected = None, ammissibilita = None, isNewRoute = true),
      Rgl(service = "RGL", pdr = "pdr4", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("03/03/2021")), motivation = Some(3), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"),
        collected = None, ammissibilita = None, isNewRoute = true),
      Rgl(service = "RGL", pdr = "pdr4", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("04/04/2021")), motivation = Some(3), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"),
        collected = None, ammissibilita = None, isNewRoute = true),
      // Case 1: they are grouped by pdr, service, date, localFile (there are three groups, 1-2, 3-4, 5): the first group is discarded since motivation is different. Result => 3 flows
      // Case 2: the remaining flows are grouped by timestampLocalFile and fileName is checked. Result => 3 flows
      // Case 3: the remaining flows are grouped by fileName, and localFiles are checked. Since they are different, each flow with inconsistent pivaUtente is discarded. Result => 2 flows
      // Case 4: the remaining flows are grouped by localFile, and everything but dCaricamento is checked. Then the most recent flow is chosen. Result => 1 flow (the chosen flow is random, since dCaricamento is None)
      Rml(service = "RML", pdr = "pdr5", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), d_caricamento = None, motivation = Some(5), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"),
        collected = None, ammissibilita = None, isNewRoute = true),
      Rml(service = "RML", pdr = "pdr5", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), d_caricamento = None, motivation = Some(4), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"),
        collected = None, ammissibilita = None, isNewRoute = true),
      Rml(service = "RML", pdr = "pdr5", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), d_caricamento = None, motivation = Some(3), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_piva4/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"),
        collected = None, ammissibilita = None, isNewRoute = true),
      Rml(service = "RML", pdr = "pdr5", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), d_caricamento = None, motivation = Some(3), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_piva4/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"),
        collected = None, ammissibilita = None, isNewRoute = true),
      Rml(service = "RML", pdr = "pdr5", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), d_caricamento = None, motivation = Some(3), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_piva5/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"),
        collected = None, ammissibilita = None, isNewRoute = true),

      IgmgPre(service = "IGMGPRE", pdr = "pdr6", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("01/01/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151501_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0), ammissibilita = None, isNewRoute = true),
      IgmgPost(service = "IGMGPOST", pdr = "pdr6", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), d_caricamento = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151501_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0), ammissibilita = None, isNewRoute = true),
      IgmgPre(service = "IGMGPRE", pdr = "pdr6", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), d_caricamento = None, local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151501_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0), ammissibilita = None, isNewRoute = true),
      IgmgPost(service = "IGMGPOST", pdr = "pdr6", date = Some(date), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), d_caricamento = Some(italianFormatter.parse("01/01/2021")), local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151501_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0), ammissibilita = None, isNewRoute = true)
    ))

    val result = IngestionFilter.removeDuplicateFlows(measures)

    Assert.assertEquals(0, result.filter(f => f.isInstanceOf[Tgl]).count)
    Assert.assertEquals(2, result.filter(f => f.isInstanceOf[A01]).count)
    Assert.assertEquals(0, result.filter(f => f.isInstanceOf[AD2]).count)
    Assert.assertEquals(2, result.filter(f => f.isInstanceOf[AD3]).count)
    Assert.assertEquals(1, result.filter(f => f.isInstanceOf[Rml]).count)
    Assert.assertEquals(1, result.filter(f => f.isInstanceOf[Rgl]).count)
    Assert.assertEquals(1, result.filter(f => f.isInstanceOf[IgmgPre]).count)
    Assert.assertEquals(1, result.filter(f => f.isInstanceOf[IgmgPost]).count)
  }
}

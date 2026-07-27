package it.eng.au.eng.ammissibilita

import it.eng.au.ammissibilita.pod.CheckAmmissibilitaPod
import it.eng.au.model.ReportEsitoPODMessage
import it.eng.au.utility.Constants
import junit.framework.TestCase
import org.junit.Assert

class testMessagesNewLogic extends TestCase{

  def testGetMessageWithNewLogic():Unit={
    val cartellaCloud1G = "/mnt/nfs/isilonshare1/TMP_1G/isilonshare1G/TME_05779711000/DISTRIBUTORE/TME_05779711000_99900020158/2020/1215"
    val expectedCartellaCloud1G = "TME_05779711000/DISTRIBUTORE/TME_05779711000_99900020158/2020/1215"

    val podMessage1GOK = ReportEsitoPODMessage(cartellaCloud =cartellaCloud1G, ammissibilita = Constants.POD, bloccante = Constants.OK )
    val podMessage1GBloccante = ReportEsitoPODMessage(cartellaCloud =cartellaCloud1G, ammissibilita = Constants.POD, bloccante = Constants.BLOCCANTE )
    val podMessage1GNonBloccante = ReportEsitoPODMessage(cartellaCloud =cartellaCloud1G, ammissibilita = Constants.POD, bloccante = Constants.NON_BLOCCANTE )
    val fileMessage1GOK = ReportEsitoPODMessage(cartellaCloud =cartellaCloud1G, ammissibilita = Constants.FILE, bloccante = Constants.OK )
    val fileMessage1GBloccante = ReportEsitoPODMessage(cartellaCloud =cartellaCloud1G, ammissibilita = Constants.FILE, bloccante = Constants.BLOCCANTE )
    val fileMessage1GNonBloccante = ReportEsitoPODMessage(cartellaCloud =cartellaCloud1G, ammissibilita = Constants.FILE, bloccante = Constants.NON_BLOCCANTE )

    val expectedPodMessage1GOK = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud1G, ammissibilita = Constants.YES, bloccante = Constants.NO )
    val expectedPodMessage1GBloccante = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud1G, ammissibilita = Constants.NO, bloccante = Constants.YES )
    val expectedPodMessage1GNonBloccante = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud1G, ammissibilita = Constants.NO, bloccante = Constants.NO )
    val expectedFileMessage1GOK = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud1G, ammissibilita = Constants.YES, bloccante = Constants.NO )
    val expectedFileMessage1GBloccante = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud1G, ammissibilita = Constants.NO, bloccante = Constants.YES )
    val expectedFileMessage1GNonBloccante = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud1G, ammissibilita = Constants.NO, bloccante = Constants.NO )

    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(podMessage1GOK),expectedPodMessage1GOK)
    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(podMessage1GBloccante),expectedPodMessage1GBloccante)
    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(podMessage1GNonBloccante),expectedPodMessage1GNonBloccante)
    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(fileMessage1GOK),expectedFileMessage1GOK)
    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(fileMessage1GBloccante),expectedFileMessage1GBloccante)
    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(fileMessage1GNonBloccante),expectedFileMessage1GNonBloccante)


    val cartellaCloud2G = "/mnt/nfs/isilonshare1/TMP_2G/isilonshare/TM2G_05779711000/DISTRIBUTORE/2G_05779711000_12300020158/2020/1215"
    val expectedCartellaCloud2G = "TM2G_05779711000/DISTRIBUTORE/2G_05779711000_12300020158/2020/1215"

    val podMessage2GOK = ReportEsitoPODMessage(cartellaCloud =cartellaCloud2G, ammissibilita = Constants.POD, bloccante = Constants.OK )
    val podMessage2GBloccante = ReportEsitoPODMessage(cartellaCloud =cartellaCloud2G, ammissibilita = Constants.POD, bloccante = Constants.BLOCCANTE )
    val podMessage2GNonBloccante = ReportEsitoPODMessage(cartellaCloud =cartellaCloud2G, ammissibilita = Constants.POD, bloccante = Constants.NON_BLOCCANTE )
    val fileMessage2GOK = ReportEsitoPODMessage(cartellaCloud =cartellaCloud2G, ammissibilita = Constants.FILE, bloccante = Constants.OK )
    val fileMessage2GBloccante = ReportEsitoPODMessage(cartellaCloud =cartellaCloud2G, ammissibilita = Constants.FILE, bloccante = Constants.BLOCCANTE )
    val fileMessage2GNonBloccante = ReportEsitoPODMessage(cartellaCloud =cartellaCloud2G, ammissibilita = Constants.FILE, bloccante = Constants.NON_BLOCCANTE )

    val expectedPodMessage2GOK = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud2G, ammissibilita = Constants.YES, bloccante = Constants.NO )
    val expectedPodMessage2GBloccante = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud2G, ammissibilita = Constants.NO, bloccante = Constants.YES )
    val expectedPodMessage2GNonBloccante = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud2G, ammissibilita = Constants.NO, bloccante = Constants.NO )
    val expectedFileMessage2GOK = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud2G, ammissibilita = Constants.YES, bloccante = Constants.NO )
    val expectedFileMessage2GBloccante = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud2G, ammissibilita = Constants.NO, bloccante = Constants.YES )
    val expectedFileMessage2GNonBloccante = ReportEsitoPODMessage(cartellaCloud =expectedCartellaCloud2G, ammissibilita = Constants.NO, bloccante = Constants.NO )

    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(podMessage2GOK),expectedPodMessage2GOK)
    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(podMessage2GBloccante),expectedPodMessage2GBloccante)
    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(podMessage2GNonBloccante),expectedPodMessage2GNonBloccante)
    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(fileMessage2GOK),expectedFileMessage2GOK)
    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(fileMessage2GBloccante),expectedFileMessage2GBloccante)
    Assert.assertEquals(CheckAmmissibilitaPod.getMessageWithNewLogic(fileMessage2GNonBloccante),expectedFileMessage2GNonBloccante)

  }
}

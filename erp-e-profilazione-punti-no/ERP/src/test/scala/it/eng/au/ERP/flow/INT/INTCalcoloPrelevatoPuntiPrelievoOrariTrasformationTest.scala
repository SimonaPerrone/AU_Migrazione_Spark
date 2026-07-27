package it.eng.au.ERP.flow.INT

import it.eng.au.ERP.EnvironmentSparkTest
import it.eng.au.ERP.model.au.{flussoMisureInterconnessione1Model, flussoMisureInterconnessione2Model, flussoMisureInterconnessione3Model, vAggregazioneMisureQuartorarie1Model}
import it.eng.au.ERP.model.erp.{erpValidatedINT1Model, erpValidatedINT2Model, erpValidatedINT3Model}
import it.eng.au.ERP.model.rcu.{RcuAziendaPModel, RcuPodInterconnesionePModel, rcuPodPModel}
import it.eng.au.ERP.trasformations.DIST.CalcoloPrelevatoPuntiPrelievoOrariDISTTrasformation
import it.eng.au.ERP.trasformations.INT.CalcoloPrelevatoPuntiPrelievoOrariINTTrasformation
import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.environment.Environment
import org.junit.Ignore

class INTCalcoloPrelevatoPuntiPrelievoOrariTrasformationTest  extends EnvironmentSparkTest{
 implicit  val spark = Environment.getSpark

  import spark.implicits._

//  @Ignore("not ready yet")
  def testDISTCalcoloPrelevatoPuntiPrelievoOrariTrasformationTest(): Unit = {
    // Create an instance with all doubles = 1.0
    import spark.implicits._

    // Create an instance with everything populated
    val modelInstance =
      Seq(
        flussoMisureInterconnessione1Model(
          identificativiflusso = "flusso1",
          pivagdrmis = "piva1",
          pivagdrinst = "piva1",
          pivagdralt = "piva1",
          datipod = "datipod1",
          pod = "pod1",
          idcabinaprimaria = "cabina1",
          mese = "07",
          anno = "2025",
          datipdp = "datipdp1",
          trattamento = "trattamento1",
          tensione = "1.0",
          forfait = "forfait1",
          gruppomis = "gruppo1",
          ka = "1.0",
          kr = "1.0",
          kp = "1.0",
          coefficienteperditaprel = "1.0",
          coefficienteperditaimm = "1.0",
          raccolta = "raccolta1",
          tipodato = "A",
          validato = "S",
          potmax = "1.0",
          ea = "1.0",
          ea_dst = "1.0",
          ea_e1 = "1.0", ea_e2 = "1.0", ea_e3 = "1.0", ea_e4 = "1.0",
          ea_e5 = "1.0", ea_e6 = "1.0", ea_e7 = "1.0", ea_e8 = "1.0",
          ea_e9 = "1.0", ea_e10 = "1.0", ea_e11 = "1.0", ea_e12 = "1.0",
          ea_e13 = "1.0", ea_e14 = "1.0", ea_e15 = "1.0", ea_e16 = "1.0",
          ea_e17 = "1.0", ea_e18 = "1.0", ea_e19 = "1.0", ea_e20 = "1.0",
          ea_e21 = "1.0", ea_e22 = "1.0", ea_e23 = "1.0", ea_e24 = "1.0",
          ea_e25 = "1.0", ea_e26 = "1.0", ea_e27 = "1.0", ea_e28 = "1.0",
          ea_e29 = "1.0", ea_e30 = "1.0", ea_e31 = "1.0", ea_e32 = "1.0",
          ea_e33 = "1.0", ea_e34 = "1.0", ea_e35 = "1.0", ea_e36 = "1.0",
          ea_e37 = "1.0", ea_e38 = "1.0", ea_e39 = "1.0", ea_e40 = "1.0",
          ea_e41 = "1.0", ea_e42 = "1.0", ea_e43 = "1.0", ea_e44 = "1.0",
          ea_e45 = "1.0", ea_e46 = "1.0",
          joinMod = "join1"
        ),
        flussoMisureInterconnessione1Model(
          identificativiflusso = "flusso1",
          pivagdrmis = "piva1",
          pivagdrinst = "piva1",
          pivagdralt = "piva1",
          datipod = "datipod1",
          pod = "pod1",
          idcabinaprimaria = "cabina1",
          mese = "07",
          anno = "2025",
          datipdp = "datipdp1",
          trattamento = "trattamento1",
          tensione = "1.0",
          forfait = "forfait1",
          gruppomis = "gruppo1",
          ka = "1.0",
          kr = "1.0",
          kp = "1.0",
          coefficienteperditaprel = "1.0",
          coefficienteperditaimm = "1.0",
          raccolta = "raccolta1",
          tipodato = "A",
          validato = "S",
          potmax = "1.0",
          ea = "1.0",
          ea_dst = "1.0",
          ea_e1 = "1.0", ea_e2 = "1.0", ea_e3 = "1.0", ea_e4 = "1.0",
          ea_e5 = "1.0", ea_e6 = "1.0", ea_e7 = "1.0", ea_e8 = "1.0",
          ea_e9 = "1.0", ea_e10 = "1.0", ea_e11 = "1.0", ea_e12 = "1.0",
          ea_e13 = "1.0", ea_e14 = "1.0", ea_e15 = "1.0", ea_e16 = "1.0",
          ea_e17 = "1.0", ea_e18 = "1.0", ea_e19 = "1.0", ea_e20 = "1.0",
          ea_e21 = "1.0", ea_e22 = "1.0", ea_e23 = "1.0", ea_e24 = "1.0",
          ea_e25 = "1.0", ea_e26 = "1.0", ea_e27 = "1.0", ea_e28 = "1.0",
          ea_e29 = "1.0", ea_e30 = "1.0", ea_e31 = "1.0", ea_e32 = "1.0",
          ea_e33 = "1.0", ea_e34 = "1.0", ea_e35 = "1.0", ea_e36 = "1.0",
          ea_e37 = "1.0", ea_e38 = "1.0", ea_e39 = "1.0", ea_e40 = "1.0",
          ea_e41 = "1.0", ea_e42 = "1.0", ea_e43 = "1.0", ea_e44 = "1.0",
          ea_e45 = "1.0", ea_e46 = "1.0",
          joinMod = "join2"
        ),
        flussoMisureInterconnessione1Model(
          identificativiflusso = "flusso1",
          pivagdrmis = "piva1",
          pivagdrinst = "piva1",
          pivagdralt = "piva1",
          datipod = "datipod1",
          pod = "pod1",
          idcabinaprimaria = "cabina1",
          mese = "07",
          anno = "2025",
          datipdp = "datipdp1",
          trattamento = "trattamento1",
          tensione = "1.0",
          forfait = "forfait1",
          gruppomis = "gruppo1",
          ka = "1.0",
          kr = "1.0",
          kp = "1.0",
          coefficienteperditaprel = "1.0",
          coefficienteperditaimm = "1.0",
          raccolta = "raccolta1",
          tipodato = "A",
          validato = "S",
          potmax = "1.0",
          ea = "1.0",
          ea_dst = "1.0",
          ea_e1 = "1.0", ea_e2 = "1.0", ea_e3 = "1.0", ea_e4 = "1.0",
          ea_e5 = "1.0", ea_e6 = "1.0", ea_e7 = "1.0", ea_e8 = "1.0",
          ea_e9 = "1.0", ea_e10 = "1.0", ea_e11 = "1.0", ea_e12 = "1.0",
          ea_e13 = "1.0", ea_e14 = "1.0", ea_e15 = "1.0", ea_e16 = "1.0",
          ea_e17 = "1.0", ea_e18 = "1.0", ea_e19 = "1.0", ea_e20 = "1.0",
          ea_e21 = "1.0", ea_e22 = "1.0", ea_e23 = "1.0", ea_e24 = "1.0",
          ea_e25 = "1.0", ea_e26 = "1.0", ea_e27 = "1.0", ea_e28 = "1.0",
          ea_e29 = "1.0", ea_e30 = "1.0", ea_e31 = "1.0", ea_e32 = "1.0",
          ea_e33 = "1.0", ea_e34 = "1.0", ea_e35 = "1.0", ea_e36 = "1.0",
          ea_e37 = "1.0", ea_e38 = "1.0", ea_e39 = "1.0", ea_e40 = "1.0",
          ea_e41 = "1.0", ea_e42 = "1.0", ea_e43 = "1.0", ea_e44 = "1.0",
          ea_e45 = "1.0", ea_e46 = "1.0",
          joinMod = "join3"
        )

      ).toDF

    // Create the instance
    val modelInstance2 = Seq(
      flussoMisureInterconnessione2Model(
        ea_e47 = "1.0", ea_e48 = "1.0", ea_e49 = "1.0", ea_e50 = "1.0",
        ea_e51 = "1.0", ea_e52 = "1.0", ea_e53 = "1.0", ea_e54 = "1.0",
        ea_e55 = "1.0", ea_e56 = "1.0", ea_e57 = "1.0", ea_e58 = "1.0",
        ea_e59 = "1.0", ea_e60 = "1.0", ea_e61 = "1.0", ea_e62 = "1.0",
        ea_e63 = "1.0", ea_e64 = "1.0", ea_e65 = "1.0", ea_e66 = "1.0",
        ea_e67 = "1.0", ea_e68 = "1.0", ea_e69 = "1.0", ea_e70 = "1.0",
        ea_e71 = "1.0", ea_e72 = "1.0", ea_e73 = "1.0", ea_e74 = "1.0",
        ea_e75 = "1.0", ea_e76 = "1.0", ea_e77 = "1.0", ea_e78 = "1.0",
        ea_e79 = "1.0", ea_e80 = "1.0", ea_e81 = "1.0", ea_e82 = "1.0",
        ea_e83 = "1.0", ea_e84 = "1.0", ea_e85 = "1.0", ea_e86 = "1.0",
        ea_e87 = "1.0", ea_e88 = "1.0", ea_e89 = "1.0", ea_e90 = "1.0",
        ea_e91 = "1.0", ea_e92 = "1.0", ea_e93 = "1.0", ea_e94 = "1.0",
        ea_e95 = "1.0", ea_e96 = "1.0", ea_e97 = "1.0", ea_e98 = "1.0",
        ea_e99 = "1.0", ea_e100 = "1.0",

        eaint_e1 = "1.0", eaint_e2 = "1.0", eaint_e3 = "1.0", eaint_e4 = "1.0",
        eaint_e5 = "1.0", eaint_e6 = "1.0", eaint_e7 = "1.0", eaint_e8 = "1.0",
        eaint_e9 = "1.0", eaint_e10 = "1.0", eaint_e11 = "1.0", eaint_e12 = "1.0",
        eaint_e13 = "1.0", eaint_e14 = "1.0", eaint_e15 = "1.0", eaint_e16 = "1.0",
        eaint_e17 = "1.0",

        joinMod = "join1"
      ),
      flussoMisureInterconnessione2Model(
        ea_e47 = "1.0", ea_e48 = "1.0", ea_e49 = "1.0", ea_e50 = "1.0",
        ea_e51 = "1.0", ea_e52 = "1.0", ea_e53 = "1.0", ea_e54 = "1.0",
        ea_e55 = "1.0", ea_e56 = "1.0", ea_e57 = "1.0", ea_e58 = "1.0",
        ea_e59 = "1.0", ea_e60 = "1.0", ea_e61 = "1.0", ea_e62 = "1.0",
        ea_e63 = "1.0", ea_e64 = "1.0", ea_e65 = "1.0", ea_e66 = "1.0",
        ea_e67 = "1.0", ea_e68 = "1.0", ea_e69 = "1.0", ea_e70 = "1.0",
        ea_e71 = "1.0", ea_e72 = "1.0", ea_e73 = "1.0", ea_e74 = "1.0",
        ea_e75 = "1.0", ea_e76 = "1.0", ea_e77 = "1.0", ea_e78 = "1.0",
        ea_e79 = "1.0", ea_e80 = "1.0", ea_e81 = "1.0", ea_e82 = "1.0",
        ea_e83 = "1.0", ea_e84 = "1.0", ea_e85 = "1.0", ea_e86 = "1.0",
        ea_e87 = "1.0", ea_e88 = "1.0", ea_e89 = "1.0", ea_e90 = "1.0",
        ea_e91 = "1.0", ea_e92 = "1.0", ea_e93 = "1.0", ea_e94 = "1.0",
        ea_e95 = "1.0", ea_e96 = "1.0", ea_e97 = "1.0", ea_e98 = "1.0",
        ea_e99 = "1.0", ea_e100 = "1.0",

        eaint_e1 = "1.0", eaint_e2 = "1.0", eaint_e3 = "1.0", eaint_e4 = "1.0",
        eaint_e5 = "1.0", eaint_e6 = "1.0", eaint_e7 = "1.0", eaint_e8 = "1.0",
        eaint_e9 = "1.0", eaint_e10 = "1.0", eaint_e11 = "1.0", eaint_e12 = "1.0",
        eaint_e13 = "1.0", eaint_e14 = "1.0", eaint_e15 = "1.0", eaint_e16 = "1.0",
        eaint_e17 = "1.0",

        joinMod = "join2"
      ),
      flussoMisureInterconnessione2Model(
        ea_e47 = "1.0", ea_e48 = "1.0", ea_e49 = "1.0", ea_e50 = "1.0",
        ea_e51 = "1.0", ea_e52 = "1.0", ea_e53 = "1.0", ea_e54 = "1.0",
        ea_e55 = "1.0", ea_e56 = "1.0", ea_e57 = "1.0", ea_e58 = "1.0",
        ea_e59 = "1.0", ea_e60 = "1.0", ea_e61 = "1.0", ea_e62 = "1.0",
        ea_e63 = "1.0", ea_e64 = "1.0", ea_e65 = "1.0", ea_e66 = "1.0",
        ea_e67 = "1.0", ea_e68 = "1.0", ea_e69 = "1.0", ea_e70 = "1.0",
        ea_e71 = "1.0", ea_e72 = "1.0", ea_e73 = "1.0", ea_e74 = "1.0",
        ea_e75 = "1.0", ea_e76 = "1.0", ea_e77 = "1.0", ea_e78 = "1.0",
        ea_e79 = "1.0", ea_e80 = "1.0", ea_e81 = "1.0", ea_e82 = "1.0",
        ea_e83 = "1.0", ea_e84 = "1.0", ea_e85 = "1.0", ea_e86 = "1.0",
        ea_e87 = "1.0", ea_e88 = "1.0", ea_e89 = "1.0", ea_e90 = "1.0",
        ea_e91 = "1.0", ea_e92 = "1.0", ea_e93 = "1.0", ea_e94 = "1.0",
        ea_e95 = "1.0", ea_e96 = "1.0", ea_e97 = "1.0", ea_e98 = "1.0",
        ea_e99 = "1.0", ea_e100 = "1.0",

        eaint_e1 = "1.0", eaint_e2 = "1.0", eaint_e3 = "1.0", eaint_e4 = "1.0",
        eaint_e5 = "1.0", eaint_e6 = "1.0", eaint_e7 = "1.0", eaint_e8 = "1.0",
        eaint_e9 = "1.0", eaint_e10 = "1.0", eaint_e11 = "1.0", eaint_e12 = "1.0",
        eaint_e13 = "1.0", eaint_e14 = "1.0", eaint_e15 = "1.0", eaint_e16 = "1.0",
        eaint_e17 = "1.0",

        joinMod = "join3"
      )
    ).toDF

    // Create the instance
    val modelInstance3 = Seq(
      flussoMisureInterconnessione3Model(
        joinMod = "join1",

        eaint_e18 = "1.0", eaint_e19 = "1.0", eaint_e20 = "1.0", eaint_e21 = "1.0",
        eaint_e22 = "1.0", eaint_e23 = "1.0", eaint_e24 = "1.0", eaint_e25 = "1.0",
        eaint_e26 = "1.0", eaint_e27 = "1.0", eaint_e28 = "1.0", eaint_e29 = "1.0",
        eaint_e30 = "1.0", eaint_e31 = "1.0", eaint_e32 = "1.0", eaint_e33 = "1.0",
        eaint_e34 = "1.0", eaint_e35 = "1.0", eaint_e36 = "1.0", eaint_e37 = "1.0",
        eaint_e38 = "1.0", eaint_e39 = "1.0", eaint_e40 = "1.0", eaint_e41 = "1.0",
        eaint_e42 = "1.0", eaint_e43 = "1.0", eaint_e44 = "1.0", eaint_e45 = "1.0",
        eaint_e46 = "1.0", eaint_e47 = "1.0", eaint_e48 = "1.0", eaint_e49 = "1.0",
        eaint_e50 = "1.0", eaint_e51 = "1.0", eaint_e52 = "1.0", eaint_e53 = "1.0",
        eaint_e54 = "1.0", eaint_e55 = "1.0", eaint_e56 = "1.0", eaint_e57 = "1.0",
        eaint_e58 = "1.0", eaint_e59 = "1.0", eaint_e60 = "1.0", eaint_e61 = "1.0",
        eaint_e62 = "1.0", eaint_e63 = "1.0", eaint_e64 = "1.0", eaint_e65 = "1.0",
        eaint_e66 = "1.0", eaint_e67 = "1.0", eaint_e68 = "1.0", eaint_e69 = "1.0",
        eaint_e70 = "1.0", eaint_e71 = "1.0", eaint_e72 = "1.0", eaint_e73 = "1.0",
        eaint_e74 = "1.0", eaint_e75 = "1.0", eaint_e76 = "1.0", eaint_e77 = "1.0",
        eaint_e78 = "1.0", eaint_e79 = "1.0", eaint_e80 = "1.0", eaint_e81 = "1.0",
        eaint_e82 = "1.0", eaint_e83 = "1.0", eaint_e84 = "1.0", eaint_e85 = "1.0",
        eaint_e86 = "1.0", eaint_e87 = "1.0", eaint_e88 = "1.0", eaint_e89 = "1.0",
        eaint_e90 = "1.0", eaint_e91 = "1.0", eaint_e92 = "1.0", eaint_e93 = "1.0",
        eaint_e94 = "1.0", eaint_e95 = "1.0", eaint_e96 = "1.0", eaint_e97 = "1.0",
        eaint_e98 = "1.0", eaint_e99 = "1.0", eaint_e100 = "1.0",

        destinatario = "dest1",
        nomefile = "file1.csv",
        annomesegiornodir = "20250709",
        time_stamp = "20250127006459"
      ),
      flussoMisureInterconnessione3Model(
        joinMod = "join2",

        eaint_e18 = "1.0", eaint_e19 = "1.0", eaint_e20 = "1.0", eaint_e21 = "1.0",
        eaint_e22 = "1.0", eaint_e23 = "1.0", eaint_e24 = "1.0", eaint_e25 = "1.0",
        eaint_e26 = "1.0", eaint_e27 = "1.0", eaint_e28 = "1.0", eaint_e29 = "1.0",
        eaint_e30 = "1.0", eaint_e31 = "1.0", eaint_e32 = "1.0", eaint_e33 = "1.0",
        eaint_e34 = "1.0", eaint_e35 = "1.0", eaint_e36 = "1.0", eaint_e37 = "1.0",
        eaint_e38 = "1.0", eaint_e39 = "1.0", eaint_e40 = "1.0", eaint_e41 = "1.0",
        eaint_e42 = "1.0", eaint_e43 = "1.0", eaint_e44 = "1.0", eaint_e45 = "1.0",
        eaint_e46 = "1.0", eaint_e47 = "1.0", eaint_e48 = "1.0", eaint_e49 = "1.0",
        eaint_e50 = "1.0", eaint_e51 = "1.0", eaint_e52 = "1.0", eaint_e53 = "1.0",
        eaint_e54 = "1.0", eaint_e55 = "1.0", eaint_e56 = "1.0", eaint_e57 = "1.0",
        eaint_e58 = "1.0", eaint_e59 = "1.0", eaint_e60 = "1.0", eaint_e61 = "1.0",
        eaint_e62 = "1.0", eaint_e63 = "1.0", eaint_e64 = "1.0", eaint_e65 = "1.0",
        eaint_e66 = "1.0", eaint_e67 = "1.0", eaint_e68 = "1.0", eaint_e69 = "1.0",
        eaint_e70 = "1.0", eaint_e71 = "1.0", eaint_e72 = "1.0", eaint_e73 = "1.0",
        eaint_e74 = "1.0", eaint_e75 = "1.0", eaint_e76 = "1.0", eaint_e77 = "1.0",
        eaint_e78 = "1.0", eaint_e79 = "1.0", eaint_e80 = "1.0", eaint_e81 = "1.0",
        eaint_e82 = "1.0", eaint_e83 = "1.0", eaint_e84 = "1.0", eaint_e85 = "1.0",
        eaint_e86 = "1.0", eaint_e87 = "1.0", eaint_e88 = "1.0", eaint_e89 = "1.0",
        eaint_e90 = "1.0", eaint_e91 = "1.0", eaint_e92 = "1.0", eaint_e93 = "1.0",
        eaint_e94 = "1.0", eaint_e95 = "1.0", eaint_e96 = "1.0", eaint_e97 = "1.0",
        eaint_e98 = "1.0", eaint_e99 = "1.0", eaint_e100 = "1.0",

        destinatario = "dest1",
        nomefile = "file1.csv",
        annomesegiornodir = "20250709",
        time_stamp = "20250127003459"
      ),
      flussoMisureInterconnessione3Model(
        joinMod = "join3",

        eaint_e18 = "1.0", eaint_e19 = "1.0", eaint_e20 = "1.0", eaint_e21 = "1.0",
        eaint_e22 = "1.0", eaint_e23 = "1.0", eaint_e24 = "1.0", eaint_e25 = "1.0",
        eaint_e26 = "1.0", eaint_e27 = "1.0", eaint_e28 = "1.0", eaint_e29 = "1.0",
        eaint_e30 = "1.0", eaint_e31 = "1.0", eaint_e32 = "1.0", eaint_e33 = "1.0",
        eaint_e34 = "1.0", eaint_e35 = "1.0", eaint_e36 = "1.0", eaint_e37 = "1.0",
        eaint_e38 = "1.0", eaint_e39 = "1.0", eaint_e40 = "1.0", eaint_e41 = "1.0",
        eaint_e42 = "1.0", eaint_e43 = "1.0", eaint_e44 = "1.0", eaint_e45 = "1.0",
        eaint_e46 = "1.0", eaint_e47 = "1.0", eaint_e48 = "1.0", eaint_e49 = "1.0",
        eaint_e50 = "1.0", eaint_e51 = "1.0", eaint_e52 = "1.0", eaint_e53 = "1.0",
        eaint_e54 = "1.0", eaint_e55 = "1.0", eaint_e56 = "1.0", eaint_e57 = "1.0",
        eaint_e58 = "1.0", eaint_e59 = "1.0", eaint_e60 = "1.0", eaint_e61 = "1.0",
        eaint_e62 = "1.0", eaint_e63 = "1.0", eaint_e64 = "1.0", eaint_e65 = "1.0",
        eaint_e66 = "1.0", eaint_e67 = "1.0", eaint_e68 = "1.0", eaint_e69 = "1.0",
        eaint_e70 = "1.0", eaint_e71 = "1.0", eaint_e72 = "1.0", eaint_e73 = "1.0",
        eaint_e74 = "1.0", eaint_e75 = "1.0", eaint_e76 = "1.0", eaint_e77 = "1.0",
        eaint_e78 = "1.0", eaint_e79 = "1.0", eaint_e80 = "1.0", eaint_e81 = "1.0",
        eaint_e82 = "1.0", eaint_e83 = "1.0", eaint_e84 = "1.0", eaint_e85 = "1.0",
        eaint_e86 = "1.0", eaint_e87 = "1.0", eaint_e88 = "1.0", eaint_e89 = "1.0",
        eaint_e90 = "1.0", eaint_e91 = "1.0", eaint_e92 = "1.0", eaint_e93 = "1.0",
        eaint_e94 = "1.0", eaint_e95 = "1.0", eaint_e96 = "1.0", eaint_e97 = "1.0",
        eaint_e98 = "1.0", eaint_e99 = "1.0", eaint_e100 = "1.0",

        destinatario = "dest1",
        nomefile = "file1.csv",
        annomesegiornodir = "20250709",
        time_stamp = "20250127003259"
      )
    ).toDF

    val rcuPodInterconnesionePIstance = Seq(
      RcuPodInterconnesionePModel(n_id_pod = "pod1",t_area_rif = "area1")
    ).toDS

    val podExcluded : List[String] = Seq("pod3").toList
    val timestamp = System.currentTimeMillis()
    val arg = ERPArgsConfig()
    val currentPartitions = 2
    val year = Some(2025)
    val month = Some(7)
    val area=Some("area1")
    val singola_piva_distributore = Some("98765432109")

    val modelInstanceJoin = modelInstance
      .join(modelInstance2,modelInstance2("joinMod") === modelInstance("joinMod"),"inner")
      .join(modelInstance3,modelInstance3("joinMod") === modelInstance("joinMod"),"inner")

    modelInstanceJoin.show()

    val finalDf = CalcoloPrelevatoPuntiPrelievoOrariINTTrasformation.calcoloErpValidatedInt(
      modelInstanceJoin,
      rcuPodInterconnesionePIstance,
      arg,
      currentPartitions,
      podExcluded,
      timestamp,
      year,
      month,
      area,
      singola_piva_distributore
    )

    finalDf.show()

  }
  }

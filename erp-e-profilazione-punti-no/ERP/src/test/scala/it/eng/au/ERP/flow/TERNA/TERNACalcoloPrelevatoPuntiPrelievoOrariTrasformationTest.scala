package it.eng.au.ERP.flow.TERNA

import it.eng.au.ERP.EnvironmentSparkTest
import it.eng.au.ERP.model.au.{aggregazioneMisureQuartorarie1Model, podConnessioneModel}
import it.eng.au.ERP.model.rcu.RcuAziendaPModel
import it.eng.au.ERP.trasformations.TERNA.CalcoloPrelevatoPuntiPrelievoOrariTERNATrasformation
import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.environment.Environment

class TERNACalcoloPrelevatoPuntiPrelievoOrariTrasformationTest  extends EnvironmentSparkTest{
 implicit  val spark = Environment.getSpark

  import spark.implicits._

  def testDISTCalcoloPrelevatoPuntiPrelievoOrariTrasformationTest(): Unit = {
    // Create an instance with all doubles = 1.0
    val aggregazioneMisureQuartorarie1Rows = Seq(
      //user 1,
      aggregazioneMisureQuartorarie1Model(
        pivautente = "user1",
        pod = "pod1",
        giorno = 1,
        area = "area1",
        validato = "Y",
        nomefile = "file1",
        codcontrdisp = "code1",
        coduc = "uc1",
        tipodato_e = 1,
        tipodato_s = 1,
        tensione = 1.0,
        trattamento_o = "treatment1",
        potcontrimpl = 1.0,
        potdisp = 1.0,
        cifreatt = 1,
        cifrerea = 1,
        raccolta = "raccolta1",
        potmax = 1.0,
        perdita = 1.0,
        annomesegiornodir = 20250101,
        h1_q1 = 1.0, h1_q2 = 1.0, h1_q3 = 1.0, h1_q4 = 1.0,
        h2_q5 = 1.0, h2_q6 = 1.0, h2_q7 = 1.0, h2_q8 = 1.0,
        h3_q9 = 1.0, h3_q10 = 1.0, h3_q11 = 1.0, h3_q12 = 1.0,
        h4_q13 = 1.0, h4_q14 = 1.0, h4_q15 = 1.0, h4_q16 = 1.0,
        h5_q17 = 1.0, h5_q18 = 1.0, h5_q19 = 1.0, h5_q20 = 1.0,
        h6_q21 = 1.0, h6_q22 = 1.0, h6_q23 = 1.0, h6_q24 = 1.0,
        h7_q25 = 1.0, h7_q26 = 1.0, h7_q27 = 1.0, h7_q28 = 1.0,
        h8_q29 = 1.0, h8_q30 = 1.0, h8_q31 = 1.0, h8_q32 = 1.0,
        h9_q33 = 1.0, h9_q34 = 1.0, h9_q35 = 1.0, h9_q36 = 1.0,
        h10_q37 = 1.0, h10_q38 = 1.0, h10_q39 = 1.0, h10_q40 = 1.0,
        h11_q41 = 1.0, h11_q42 = 1.0, h11_q43 = 1.0, h11_q44 = 1.0,
        h12_q45 = 1.0, h12_q46 = 1.0, h12_q47 = 1.0,
        joinModel = "join1"
      ),
      //user 2,
      aggregazioneMisureQuartorarie1Model(
        pivautente = "user2",
        pod = "pod2",
        giorno = 1,
        area = "area1",
        validato = "Y",
        nomefile = "file1",
        codcontrdisp = "code1",
        coduc = "uc1",
        tipodato_e = 1,
        tipodato_s = 1,
        tensione = 1.0,
        trattamento_o = "treatment1",
        potcontrimpl = 1.0,
        potdisp = 1.0,
        cifreatt = 1,
        cifrerea = 1,
        raccolta = "raccolta1",
        potmax = 1.0,
        perdita = 1.0,
        annomesegiornodir = 20250101,
        h1_q1 = 1.0, h1_q2 = 1.0, h1_q3 = 1.0, h1_q4 = 1.0,
        h2_q5 = 1.0, h2_q6 = 1.0, h2_q7 = 1.0, h2_q8 = 1.0,
        h3_q9 = 1.0, h3_q10 = 1.0, h3_q11 = 1.0, h3_q12 = 1.0,
        h4_q13 = 1.0, h4_q14 = 1.0, h4_q15 = 1.0, h4_q16 = 1.0,
        h5_q17 = 1.0, h5_q18 = 1.0, h5_q19 = 1.0, h5_q20 = 1.0,
        h6_q21 = 1.0, h6_q22 = 1.0, h6_q23 = 1.0, h6_q24 = 1.0,
        h7_q25 = 1.0, h7_q26 = 1.0, h7_q27 = 1.0, h7_q28 = 1.0,
        h8_q29 = 1.0, h8_q30 = 1.0, h8_q31 = 1.0, h8_q32 = 1.0,
        h9_q33 = 1.0, h9_q34 = 1.0, h9_q35 = 1.0, h9_q36 = 1.0,
        h10_q37 = 1.0, h10_q38 = 1.0, h10_q39 = 1.0, h10_q40 = 1.0,
        h11_q41 = 1.0, h11_q42 = 1.0, h11_q43 = 1.0, h11_q44 = 1.0,
        h12_q45 = 1.0, h12_q46 = 1.0, h12_q47 = 1.0,
        joinModel = "join2"
      ),
      //user 3,
      aggregazioneMisureQuartorarie1Model(
        pivautente = "user3",
        pod = "pod3",
        giorno = 1,
        area = "area1",
        validato = "Y",
        nomefile = "file1",
        codcontrdisp = "code1",
        coduc = "uc1",
        tipodato_e = 1,
        tipodato_s = 1,
        tensione = 1.0,
        trattamento_o = "treatment1",
        potcontrimpl = 1.0,
        potdisp = 1.0,
        cifreatt = 1,
        cifrerea = 1,
        raccolta = "raccolta1",
        potmax = 1.0,
        perdita = 1.0,
        annomesegiornodir = 20250101,
        h1_q1 = 1.0, h1_q2 = 1.0, h1_q3 = 1.0, h1_q4 = 1.0,
        h2_q5 = 1.0, h2_q6 = 1.0, h2_q7 = 1.0, h2_q8 = 1.0,
        h3_q9 = 1.0, h3_q10 = 1.0, h3_q11 = 1.0, h3_q12 = 1.0,
        h4_q13 = 1.0, h4_q14 = 1.0, h4_q15 = 1.0, h4_q16 = 1.0,
        h5_q17 = 1.0, h5_q18 = 1.0, h5_q19 = 1.0, h5_q20 = 1.0,
        h6_q21 = 1.0, h6_q22 = 1.0, h6_q23 = 1.0, h6_q24 = 1.0,
        h7_q25 = 1.0, h7_q26 = 1.0, h7_q27 = 1.0, h7_q28 = 1.0,
        h8_q29 = 1.0, h8_q30 = 1.0, h8_q31 = 1.0, h8_q32 = 1.0,
        h9_q33 = 1.0, h9_q34 = 1.0, h9_q35 = 1.0, h9_q36 = 1.0,
        h10_q37 = 1.0, h10_q38 = 1.0, h10_q39 = 1.0, h10_q40 = 1.0,
        h11_q41 = 1.0, h11_q42 = 1.0, h11_q43 = 1.0, h11_q44 = 1.0,
        h12_q45 = 1.0, h12_q46 = 1.0, h12_q47 = 1.0,
        joinModel = "join3"
      ),
      //user 4,
      aggregazioneMisureQuartorarie1Model(
        pivautente = "user4",
        pod = "pod4",
        giorno = 1,
        area = "area1",
        validato = "Y",
        nomefile = "file1",
        codcontrdisp = "code1",
        coduc = "uc1",
        tipodato_e = 1,
        tipodato_s = 1,
        tensione = 1.0,
        trattamento_o = "treatment1",
        potcontrimpl = 1.0,
        potdisp = 1.0,
        cifreatt = 1,
        cifrerea = 1,
        raccolta = "raccolta1",
        potmax = 1.0,
        perdita = 1.0,
        annomesegiornodir = 20250101,
        h1_q1 = 1.0, h1_q2 = 1.0, h1_q3 = 1.0, h1_q4 = 1.0,
        h2_q5 = 1.0, h2_q6 = 1.0, h2_q7 = 1.0, h2_q8 = 1.0,
        h3_q9 = 1.0, h3_q10 = 1.0, h3_q11 = 1.0, h3_q12 = 1.0,
        h4_q13 = 1.0, h4_q14 = 1.0, h4_q15 = 1.0, h4_q16 = 1.0,
        h5_q17 = 1.0, h5_q18 = 1.0, h5_q19 = 1.0, h5_q20 = 1.0,
        h6_q21 = 1.0, h6_q22 = 1.0, h6_q23 = 1.0, h6_q24 = 1.0,
        h7_q25 = 1.0, h7_q26 = 1.0, h7_q27 = 1.0, h7_q28 = 1.0,
        h8_q29 = 1.0, h8_q30 = 1.0, h8_q31 = 1.0, h8_q32 = 1.0,
        h9_q33 = 1.0, h9_q34 = 1.0, h9_q35 = 1.0, h9_q36 = 1.0,
        h10_q37 = 1.0, h10_q38 = 1.0, h10_q39 = 1.0, h10_q40 = 1.0,
        h11_q41 = 1.0, h11_q42 = 1.0, h11_q43 = 1.0, h11_q44 = 1.0,
        h12_q45 = 1.0, h12_q46 = 1.0, h12_q47 = 1.0,
        joinModel = "join4"
      ),
      //user 5,
      aggregazioneMisureQuartorarie1Model(
        pivautente = "user5",
        pod = "pod5",
        giorno = 1,
        area = "area2",
        validato = "Y",
        nomefile = "file1",
        codcontrdisp = "code1",
        coduc = "uc1",
        tipodato_e = 1,
        tipodato_s = 1,
        tensione = 1.0,
        trattamento_o = "treatment1",
        potcontrimpl = 1.0,
        potdisp = 1.0,
        cifreatt = 1,
        cifrerea = 1,
        raccolta = "raccolta1",
        potmax = 1.0,
        perdita = 1.0,
        annomesegiornodir = 20250101,
        h1_q1 = 1.0, h1_q2 = 1.0, h1_q3 = 1.0, h1_q4 = 1.0,
        h2_q5 = 1.0, h2_q6 = 1.0, h2_q7 = 1.0, h2_q8 = 1.0,
        h3_q9 = 1.0, h3_q10 = 1.0, h3_q11 = 1.0, h3_q12 = 1.0,
        h4_q13 = 1.0, h4_q14 = 1.0, h4_q15 = 1.0, h4_q16 = 1.0,
        h5_q17 = 1.0, h5_q18 = 1.0, h5_q19 = 1.0, h5_q20 = 1.0,
        h6_q21 = 1.0, h6_q22 = 1.0, h6_q23 = 1.0, h6_q24 = 1.0,
        h7_q25 = 1.0, h7_q26 = 1.0, h7_q27 = 1.0, h7_q28 = 1.0,
        h8_q29 = 1.0, h8_q30 = 1.0, h8_q31 = 1.0, h8_q32 = 1.0,
        h9_q33 = 1.0, h9_q34 = 1.0, h9_q35 = 1.0, h9_q36 = 1.0,
        h10_q37 = 1.0, h10_q38 = 1.0, h10_q39 = 1.0, h10_q40 = 1.0,
        h11_q41 = 1.0, h11_q42 = 1.0, h11_q43 = 1.0, h11_q44 = 1.0,
        h12_q45 = 1.0, h12_q46 = 1.0, h12_q47 = 1.0,
        joinModel = "join5"
      ),
      //user 5,
      aggregazioneMisureQuartorarie1Model(
        pivautente = "user6",
        pod = "pod6",
        giorno = 1,
        area = "area1",
        validato = "Y",
        nomefile = "file1",
        codcontrdisp = "code1",
        coduc = "uc1",
        tipodato_e = 1,
        tipodato_s = 1,
        tensione = 1.0,
        trattamento_o = "treatment1",
        potcontrimpl = 1.0,
        potdisp = 1.0,
        cifreatt = 1,
        cifrerea = 1,
        raccolta = "raccolta1",
        potmax = 1.0,
        perdita = 1.0,
        annomesegiornodir = 20250101,
        h1_q1 = 1.0, h1_q2 = 1.0, h1_q3 = 1.0, h1_q4 = 1.0,
        h2_q5 = 1.0, h2_q6 = 1.0, h2_q7 = 1.0, h2_q8 = 1.0,
        h3_q9 = 1.0, h3_q10 = 1.0, h3_q11 = 1.0, h3_q12 = 1.0,
        h4_q13 = 1.0, h4_q14 = 1.0, h4_q15 = 1.0, h4_q16 = 1.0,
        h5_q17 = 1.0, h5_q18 = 1.0, h5_q19 = 1.0, h5_q20 = 1.0,
        h6_q21 = 1.0, h6_q22 = 1.0, h6_q23 = 1.0, h6_q24 = 1.0,
        h7_q25 = 1.0, h7_q26 = 1.0, h7_q27 = 1.0, h7_q28 = 1.0,
        h8_q29 = 1.0, h8_q30 = 1.0, h8_q31 = 1.0, h8_q32 = 1.0,
        h9_q33 = 1.0, h9_q34 = 1.0, h9_q35 = 1.0, h9_q36 = 1.0,
        h10_q37 = 1.0, h10_q38 = 1.0, h10_q39 = 1.0, h10_q40 = 1.0,
        h11_q41 = 1.0, h11_q42 = 1.0, h11_q43 = 1.0, h11_q44 = 1.0,
        h12_q45 = 1.0, h12_q46 = 1.0, h12_q47 = 1.0,
        joinModel = "join6"
      )
    ).toDF

    import it.eng.au.ERP.model.au.aggregazioneMisureQuartorarie2Model
    import java.sql.Timestamp

    val aggregazioneMisureQuartorarie2Rows = Seq(
      //user 1
      aggregazioneMisureQuartorarie2Model(
        joinModel = "join1",
        h12_q48 = 1.0,
        h13_q49 = 1.0, h13_q50 = 1.0, h13_q51 = 1.0, h13_q52 = 1.0,
        h14_q53 = 1.0, h14_q54 = 1.0, h14_q55 = 1.0, h14_q56 = 1.0,
        h15_q57 = 1.0, h15_q58 = 1.0, h15_q59 = 1.0, h15_q60 = 1.0,
        h16_q61 = 1.0, h16_q62 = 1.0, h16_q63 = 1.0, h16_q64 = 1.0,
        h17_q65 = 1.0, h17_q66 = 1.0, h17_q67 = 1.0, h17_q68 = 1.0,
        h18_q69 = 1.0, h18_q70 = 1.0, h18_q71 = 1.0, h18_q72 = 1.0,
        h19_q73 = 1.0, h19_q74 = 1.0, h19_q75 = 1.0, h19_q76 = 1.0,
        h20_q77 = 1.0, h20_q78 = 1.0, h20_q79 = 1.0, h20_q80 = 1.0,
        h21_q81 = 1.0, h21_q82 = 1.0, h21_q83 = 1.0, h21_q84 = 1.0,
        h22_q85 = 1.0, h22_q86 = 1.0, h22_q87 = 1.0, h22_q88 = 1.0,
        h23_q89 = 1.0, h23_q90 = 1.0, h23_q91 = 1.0, h23_q92 = 1.0,
        h24_q93 = 1.0, h24_q94 = 1.0, h24_q95 = 1.0, h24_q96 = 1.0,
        h25_q97 = 1.0, h25_q98 = 1.0, h25_q99 = 1.0, h25_q100 = 1.0,
        time_stamp = BigInt(0),
        dataelaborazione = new Timestamp(System.currentTimeMillis()),
        flaguddpod = "Y",
        stato = "OK",
        trattamento = "standard",
        flagarea = "A",
        n_id_udd = "udd001",
        t_piva = "12345678901",
        n_id_distr = "dist01",
        n_id_distr_rif = "distRif01",
        flag_validazione = "valid",
        anno = 2025,
        mese = 7,
        pivadistributore = "98765432109",
        versione = BigInt(1)
      ),
      //user2
      aggregazioneMisureQuartorarie2Model(
        joinModel = "join2",
        h12_q48 = 1.0,
        h13_q49 = 1.0, h13_q50 = 1.0, h13_q51 = 1.0, h13_q52 = 1.0,
        h14_q53 = 1.0, h14_q54 = 1.0, h14_q55 = 1.0, h14_q56 = 1.0,
        h15_q57 = 1.0, h15_q58 = 1.0, h15_q59 = 1.0, h15_q60 = 1.0,
        h16_q61 = 1.0, h16_q62 = 1.0, h16_q63 = 1.0, h16_q64 = 1.0,
        h17_q65 = 1.0, h17_q66 = 1.0, h17_q67 = 1.0, h17_q68 = 1.0,
        h18_q69 = 1.0, h18_q70 = 1.0, h18_q71 = 1.0, h18_q72 = 1.0,
        h19_q73 = 1.0, h19_q74 = 1.0, h19_q75 = 1.0, h19_q76 = 1.0,
        h20_q77 = 1.0, h20_q78 = 1.0, h20_q79 = 1.0, h20_q80 = 1.0,
        h21_q81 = 1.0, h21_q82 = 1.0, h21_q83 = 1.0, h21_q84 = 1.0,
        h22_q85 = 1.0, h22_q86 = 1.0, h22_q87 = 1.0, h22_q88 = 1.0,
        h23_q89 = 1.0, h23_q90 = 1.0, h23_q91 = 1.0, h23_q92 = 1.0,
        h24_q93 = 1.0, h24_q94 = 1.0, h24_q95 = 1.0, h24_q96 = 1.0,
        h25_q97 = 1.0, h25_q98 = 1.0, h25_q99 = 1.0, h25_q100 = 1.0,
        time_stamp = BigInt(0),
        dataelaborazione = new Timestamp(System.currentTimeMillis()),
        flaguddpod = "Y",
        stato = "OK",
        trattamento = "standard",
        flagarea = "A",
        n_id_udd = "udd001",
        t_piva = "12345678901",
        n_id_distr = "dist01",
        n_id_distr_rif = "distRif01",
        flag_validazione = "valid",
        anno = 2025,
        mese = 7,
        pivadistributore = "98765432109",
        versione = BigInt(1)
      ),
      //user3
      aggregazioneMisureQuartorarie2Model(
        joinModel = "join3",
        h12_q48 = 1.0,
        h13_q49 = 1.0, h13_q50 = 1.0, h13_q51 = 1.0, h13_q52 = 1.0,
        h14_q53 = 1.0, h14_q54 = 1.0, h14_q55 = 1.0, h14_q56 = 1.0,
        h15_q57 = 1.0, h15_q58 = 1.0, h15_q59 = 1.0, h15_q60 = 1.0,
        h16_q61 = 1.0, h16_q62 = 1.0, h16_q63 = 1.0, h16_q64 = 1.0,
        h17_q65 = 1.0, h17_q66 = 1.0, h17_q67 = 1.0, h17_q68 = 1.0,
        h18_q69 = 1.0, h18_q70 = 1.0, h18_q71 = 1.0, h18_q72 = 1.0,
        h19_q73 = 1.0, h19_q74 = 1.0, h19_q75 = 1.0, h19_q76 = 1.0,
        h20_q77 = 1.0, h20_q78 = 1.0, h20_q79 = 1.0, h20_q80 = 1.0,
        h21_q81 = 1.0, h21_q82 = 1.0, h21_q83 = 1.0, h21_q84 = 1.0,
        h22_q85 = 1.0, h22_q86 = 1.0, h22_q87 = 1.0, h22_q88 = 1.0,
        h23_q89 = 1.0, h23_q90 = 1.0, h23_q91 = 1.0, h23_q92 = 1.0,
        h24_q93 = 1.0, h24_q94 = 1.0, h24_q95 = 1.0, h24_q96 = 1.0,
        h25_q97 = 1.0, h25_q98 = 1.0, h25_q99 = 1.0, h25_q100 = 1.0,
        time_stamp = BigInt(0),
        dataelaborazione = new Timestamp(System.currentTimeMillis()),
        flaguddpod = "Y",
        stato = "OK",
        trattamento = "standard",
        flagarea = "A",
        n_id_udd = "udd001",
        t_piva = "12345678901",
        n_id_distr = "dist01",
        n_id_distr_rif = "distRif01",
        flag_validazione = "valid",
        anno = 2025,
        mese = 7,
        pivadistributore = "98765432109",
        versione = BigInt(1)
      ),
      //user4
      aggregazioneMisureQuartorarie2Model(
        joinModel = "join4",
        h12_q48 = 1.0,
        h13_q49 = 1.0, h13_q50 = 1.0, h13_q51 = 1.0, h13_q52 = 1.0,
        h14_q53 = 1.0, h14_q54 = 1.0, h14_q55 = 1.0, h14_q56 = 1.0,
        h15_q57 = 1.0, h15_q58 = 1.0, h15_q59 = 1.0, h15_q60 = 1.0,
        h16_q61 = 1.0, h16_q62 = 1.0, h16_q63 = 1.0, h16_q64 = 1.0,
        h17_q65 = 1.0, h17_q66 = 1.0, h17_q67 = 1.0, h17_q68 = 1.0,
        h18_q69 = 1.0, h18_q70 = 1.0, h18_q71 = 1.0, h18_q72 = 1.0,
        h19_q73 = 1.0, h19_q74 = 1.0, h19_q75 = 1.0, h19_q76 = 1.0,
        h20_q77 = 1.0, h20_q78 = 1.0, h20_q79 = 1.0, h20_q80 = 1.0,
        h21_q81 = 1.0, h21_q82 = 1.0, h21_q83 = 1.0, h21_q84 = 1.0,
        h22_q85 = 1.0, h22_q86 = 1.0, h22_q87 = 1.0, h22_q88 = 1.0,
        h23_q89 = 1.0, h23_q90 = 1.0, h23_q91 = 1.0, h23_q92 = 1.0,
        h24_q93 = 1.0, h24_q94 = 1.0, h24_q95 = 1.0, h24_q96 = 1.0,
        h25_q97 = 1.0, h25_q98 = 1.0, h25_q99 = 1.0, h25_q100 = 1.0,
        time_stamp = BigInt(0),
        dataelaborazione = new Timestamp(System.currentTimeMillis()),
        flaguddpod = "Y",
        stato = "OK",
        trattamento = "standard",
        flagarea = "A",
        n_id_udd = "udd001",
        t_piva = "12345678901",
        n_id_distr = "dist01",
        n_id_distr_rif = "distRif01",
        flag_validazione = "valid",
        anno = 2025,
        mese = 7,
        pivadistributore = "98765432109",
        versione = BigInt(1)
      ),
      //user5
      aggregazioneMisureQuartorarie2Model(
        joinModel = "join5",
        h12_q48 = 1.0,
        h13_q49 = 1.0, h13_q50 = 1.0, h13_q51 = 1.0, h13_q52 = 1.0,
        h14_q53 = 1.0, h14_q54 = 1.0, h14_q55 = 1.0, h14_q56 = 1.0,
        h15_q57 = 1.0, h15_q58 = 1.0, h15_q59 = 1.0, h15_q60 = 1.0,
        h16_q61 = 1.0, h16_q62 = 1.0, h16_q63 = 1.0, h16_q64 = 1.0,
        h17_q65 = 1.0, h17_q66 = 1.0, h17_q67 = 1.0, h17_q68 = 1.0,
        h18_q69 = 1.0, h18_q70 = 1.0, h18_q71 = 1.0, h18_q72 = 1.0,
        h19_q73 = 1.0, h19_q74 = 1.0, h19_q75 = 1.0, h19_q76 = 1.0,
        h20_q77 = 1.0, h20_q78 = 1.0, h20_q79 = 1.0, h20_q80 = 1.0,
        h21_q81 = 1.0, h21_q82 = 1.0, h21_q83 = 1.0, h21_q84 = 1.0,
        h22_q85 = 1.0, h22_q86 = 1.0, h22_q87 = 1.0, h22_q88 = 1.0,
        h23_q89 = 1.0, h23_q90 = 1.0, h23_q91 = 1.0, h23_q92 = 1.0,
        h24_q93 = 1.0, h24_q94 = 1.0, h24_q95 = 1.0, h24_q96 = 1.0,
        h25_q97 = 1.0, h25_q98 = 1.0, h25_q99 = 1.0, h25_q100 = 1.0,
        time_stamp = BigInt(0),
        dataelaborazione = new Timestamp(System.currentTimeMillis()),
        flaguddpod = "Y",
        stato = "OK",
        trattamento = "standard",
        flagarea = "A",
        n_id_udd = "udd001",
        t_piva = "12345678901",
        n_id_distr = "dist01",
        n_id_distr_rif = "distRif01",
        flag_validazione = "valid",
        anno = 2025,
        mese = 7,
        pivadistributore = "98765432109",
        versione = BigInt(1)
      ),
      //user6
      aggregazioneMisureQuartorarie2Model(
        joinModel = "join6",
        h12_q48 = 1.0,
        h13_q49 = 1.0, h13_q50 = 1.0, h13_q51 = 1.0, h13_q52 = 1.0,
        h14_q53 = 1.0, h14_q54 = 1.0, h14_q55 = 1.0, h14_q56 = 1.0,
        h15_q57 = 1.0, h15_q58 = 1.0, h15_q59 = 1.0, h15_q60 = 1.0,
        h16_q61 = 1.0, h16_q62 = 1.0, h16_q63 = 1.0, h16_q64 = 1.0,
        h17_q65 = 1.0, h17_q66 = 1.0, h17_q67 = 1.0, h17_q68 = 1.0,
        h18_q69 = 1.0, h18_q70 = 1.0, h18_q71 = 1.0, h18_q72 = 1.0,
        h19_q73 = 1.0, h19_q74 = 1.0, h19_q75 = 1.0, h19_q76 = 1.0,
        h20_q77 = 1.0, h20_q78 = 1.0, h20_q79 = 1.0, h20_q80 = 1.0,
        h21_q81 = 1.0, h21_q82 = 1.0, h21_q83 = 1.0, h21_q84 = 1.0,
        h22_q85 = 1.0, h22_q86 = 1.0, h22_q87 = 1.0, h22_q88 = 1.0,
        h23_q89 = 1.0, h23_q90 = 1.0, h23_q91 = 1.0, h23_q92 = 1.0,
        h24_q93 = 1.0, h24_q94 = 1.0, h24_q95 = 1.0, h24_q96 = 1.0,
        h25_q97 = 1.0, h25_q98 = 1.0, h25_q99 = 1.0, h25_q100 = 1.0,
        time_stamp = BigInt(0),
        dataelaborazione = new Timestamp(System.currentTimeMillis()),
        flaguddpod = "Y",
        stato = "OK",
        trattamento = "standard",
        flagarea = "A",
        n_id_udd = "udd001",
        t_piva = "12345678901",
        n_id_distr = "dist01",
        n_id_distr_rif = "distRif01",
        flag_validazione = "valid",
        anno = 2025,
        mese = 7,
        pivadistributore = "987654321098",
        versione = BigInt(1)
      )
    ).toDF

    val podInstance = Seq(
      podConnessioneModel(
        pod14 = "pod1",
        t_connessione = "N"
      ),
      podConnessioneModel(
        pod14 = "pod2",
        t_connessione = "N"
      ),
      podConnessioneModel(
        pod14 = "pod3",
        t_connessione = "N"
      )
      ,
      podConnessioneModel(
        pod14 = "pod4",
        t_connessione = "N1"
      ),
      podConnessioneModel(
        pod14 = "pod5",
        t_connessione = "N"
      ),
      podConnessioneModel(
        pod14 = "pod6",
        t_connessione = "N"
      )
    ).toDS

    val aziendaIstance = Seq(
      RcuAziendaPModel(n_id_azienda = "dist01",t_piva = "piva1",t_rag_soc = "rag_soc1")
    ).toDS

    val podExcluded : List[String] = Seq("pod2").toList
    val timestamp = System.currentTimeMillis()
    val terna = true
    val arg = ERPArgsConfig()
    val currentPartitions = 2
    val year = Some(2025)
    val month = Some(7)
    val area = Some("area1")
    val singola_piva_distributore = Some("98765432109")

    val aggregazioneMisureQuartorarieJoined = aggregazioneMisureQuartorarie1Rows.join(
      aggregazioneMisureQuartorarie2Rows,
      aggregazioneMisureQuartorarie1Rows("joinModel") ===aggregazioneMisureQuartorarie2Rows("joinModel")
      , "inner"
    )

    val finalDf = CalcoloPrelevatoPuntiPrelievoOrariTERNATrasformation.calcoloPrelevatoPuntiPrelievoOrari(
      aggregazioneMisureQuartorarieJoined,
      aziendaIstance,
      podInstance,
      arg,
      currentPartitions,
      podExcluded,
      timestamp,
      terna,
      year,
      month,
      area,
      singola_piva_distributore
    )

    finalDf.show()

  }
  }

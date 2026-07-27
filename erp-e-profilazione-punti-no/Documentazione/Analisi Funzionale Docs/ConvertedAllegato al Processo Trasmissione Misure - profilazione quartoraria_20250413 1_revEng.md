**[SISTEMA INFORMATIVO INTEGRATO PER LA GESTIONE DEI FLUSSI INFORMATIVI
RELATIVI AI MERCATI DELL'ENERGIA ELETTRICA E DEL GAS]{.smallcaps}**

**[Profilazione quartoraria]{.smallcaps}**

**[dei consumi]{.smallcaps}**

**[non orari]{.smallcaps}**

**IN ATTUAZIONE DELLA DELIBERA**

**325/2024/R/EEL e s.m.i**

**  
**

# Sommario {#sommario .TOC-Heading}

[1. Revisione del documento
[3](#revisione-del-documento)](#revisione-del-documento)

[2. Profilazione quartoraria del consumo non orario
[4](#profilazione-quartoraria-del-consumo-non-orario)](#profilazione-quartoraria-del-consumo-non-orario)

[3. Calcolo dei Consumi [5](#calcolo-dei-consumi)](#calcolo-dei-consumi)

[3.1 Perimetro di calcolo, flussi di misura e campi di riferimento
[5](#perimetro-di-calcolo-flussi-di-misura-e-campi-di-riferimento)](#perimetro-di-calcolo-flussi-di-misura-e-campi-di-riferimento)

[3.2 Calcolo del Delta fra totalizzatori
[6](#calcolo-del-delta-fra-totalizzatori)](#calcolo-del-delta-fra-totalizzatori)

[3.2.1 Calcolo consumi in caso di SMIS
[7](#calcolo-consumi-in-caso-di-smis)](#calcolo-consumi-in-caso-di-smis)

[3.2.2 Calcolo consumi in caso di rettifiche 4/5 o Forfait
[8](#calcolo-consumi-in-caso-di-rettifiche-45-o-forfait)](#calcolo-consumi-in-caso-di-rettifiche-45-o-forfait)

[3.3 Ripartizione quartoraria del Delta fra totalizzatori
[9](#ripartizione-quartoraria-del-delta-fra-totalizzatori)](#ripartizione-quartoraria-del-delta-fra-totalizzatori)

# Indice delle tabelle {#indice-delle-tabelle .TOC-Heading}

[Tabella 1. Flussi idonei al calcolo dei consumi
[5](#_Toc195085265)](#_Toc195085265)

[Tabella 2. Campi di riferimento flussi Non Orari
[6](#_Toc195085266)](#_Toc195085266)

[Tabella 3. Esempio flussi Ricostruzione Consumi
[9](#_Toc195085267)](#_Toc195085267)

[Tabella 4. Calcolo Ricostruzione Consumi
[9](#_Toc195085268)](#_Toc195085268)

[Tabella 5. Fasce Standard Arera [10](#_Toc195085269)](#_Toc195085269)

# Revisione del documento

| **Ver**              | **Data**   | **Modifica**                      |
|----------------------|------------|-----------------------------------|
| 1.0 in consultazione | 14/04/2025 | Avvio consultazione del documento |

# Profilazione quartoraria del consumo non orario

Il SII in ottemperanza di quanto previsto all'interno della delibera
325/2024/R/EEL e s.m.i. a partire dalla **competenza di gennaio 2026**,
determina i **[consumi quartorari]{.underline}** dei punti che abbiano
all'interno del RCU trattamento diverso da orario (illuminazione
pubblica e altri punti di prelievo) e:

- li mette a disposizione puntualmente dei soggetti interessati

- li utilizza ai fini del calcolo dell'aggregato nell'ambito del
  processo AC e RS
  (<https://siiportale.acquirenteunico.it/documents/812312/0/Specifiche_Tecniche_TrasmissioneMisure_3.0>
  e
  <https://siiportale.acquirenteunico.it/documents/812312/0/Specifiche_Tecniche_Trasmissione_Rettifica_Aggregato_Misure_v3.0>
  ).

- dismette la gestione della pratica PR1, PR2, PR3
  (<https://siiportale.acquirenteunico.it/documents/343715/0/Specifiche_tecniche_trasmissione_dati_TIS_3.2>)

- dismette la gestione del minuto convenzionale di cui al paragrafo
  7.1.2 con riferimento alla ricezione dell'energia oraria convenzionale
  per POD corrispondenti ad impianti di Illuminazione Pubblica (IP) non
  trattati su base oraria
  (<https://siiportale.acquirenteunico.it/documents/812312/0/Specifiche_tecniche_TrasmissioneMisure_1.8.pdf>
  ).

Nell'ottica di semplificazione e di rapidità implementativa il SII
calcola i consumi applicando un "profilo piatto per fascia e mese"
facendo il rapporto fra l'energia elettrica prelevata nella fascia e nel
mese in cui appartiene il quarto d'ora considerato ed il numero di
quarti d'ora della medesima fascia. I consumi vengono calcolati a
partire dalla differenza fra le letture trasmesse da parte delle imprese
di distribuzione o di Terna al SII all'interno dei flussi di misura,
secondo le regole di calcolo descritte nel presente documento.

# Calcolo dei Consumi  {#calcolo-dei-consumi}

## Perimetro di calcolo, flussi di misura e campi di riferimento

L'insieme dei POD per i quali il SII effettua la profilazione
quartoraria dei consumi sono i POD che all'interno del RCU presentino
trattamento diverso da O.

I flussi idonei per il calcolo dei consumi profilati quartorari sono i
flussi trasmessi dalle ID o da Terna al SII che risultino ammissibili
secondo i controlli di ammissibilità[^1] del SII e sono riassunti nella
seguente tabella:

|           | **Cod_Flusso** | **Tipo Dato** | **Validato** |
|-----------|----------------|---------------|--------------|
| NON ORARI | PNO            | E/S           | S            |
|           | RNO[^2]        |               |              |
|           | PNO2G          | E/S           | S            |
|           | RNO2G          |               |              |
|           | SMIS           | E/S           | S            |
|           | AV, AV2G       | E/S           | S            |
|           | AVR, AVR2G     |               |              |
|           | DS, DS2G       | E/S           | S            |
|           | DSR, DSR2G     |               |              |

[]{#_Toc195085265 .anchor}Tabella 1. Flussi di misura idonei al calcolo
dei consumi

Per tutti i flussi di misura ritenuti idonei al calcolo dei consumi
riassunti in tabella 3, vengono indicati, nelle successive tabelle, i
campi di riferimento per il calcolo dei consumi (letture) e per la data
di riferimento della misura.

<table>
<colgroup>
<col style="width: 12%" />
<col style="width: 15%" />
<col style="width: 16%" />
<col style="width: 13%" />
<col style="width: 12%" />
<col style="width: 12%" />
<col style="width: 17%" />
</colgroup>
<thead>
<tr class="header">
<th colspan="7"><strong>Campi di riferimento</strong></th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td><strong>Data di riferimento</strong></td>
<td colspan="5"><strong>DataMisura</strong></td>
<td><strong>DataPrest</strong></td>
</tr>
<tr class="even">
<td><strong>Tipo flusso</strong></td>
<td><strong>PNO</strong></td>
<td><strong>RNO</strong></td>
<td><strong>PNO2G</strong></td>
<td><strong>RNO2G</strong></td>
<td><strong>SMIS</strong></td>
<td><p><strong>AV,AV2G,</strong></p>
<p><strong>DS,DS2G</strong></p></td>
</tr>
<tr class="odd">
<td rowspan="7"><strong>Consumi</strong></td>
<td>EaM</td>
<td>EaM</td>
<td>EaM</td>
<td>EaM</td>
<td>EaM</td>
<td>EaM</td>
</tr>
<tr class="even">
<td>EaF1</td>
<td>EaF1</td>
<td>EaF1</td>
<td>EaF1</td>
<td>EaF1</td>
<td>EaF1</td>
</tr>
<tr class="odd">
<td>EaF2</td>
<td>EaF2</td>
<td>EaF2</td>
<td>EaF2</td>
<td>EaF2</td>
<td>EaF2</td>
</tr>
<tr class="even">
<td>EaF3</td>
<td>EaF3</td>
<td>EaF3</td>
<td>EaF3</td>
<td>EaF3</td>
<td>EaF3</td>
</tr>
<tr class="odd">
<td></td>
<td></td>
<td>EaF4</td>
<td>EaF4</td>
<td>EaF4</td>
<td>EaF4</td>
</tr>
<tr class="even">
<td></td>
<td></td>
<td>EaF5</td>
<td>EaF5</td>
<td>EaF5</td>
<td>EaF5</td>
</tr>
<tr class="odd">
<td></td>
<td></td>
<td>EaF6</td>
<td>EaF6</td>
<td>EaF6</td>
<td>EaF6</td>
</tr>
</tbody>
</table>

[]{#_Toc195085266 .anchor}Tabella 2. Campi di riferimento flussi Non
Orari

Una volta individuati i flussi di misura utilizzabili ai fini del
calcolo dei consumi il SII calcola il **delta fra i totalizzatori** e
successivamente **ripartisce tale delta nei quarti d'ora**.

Si riportano al capitolo 3.2 le regole di calcolo del delta a partire
dai flussi di misura definiti e al capitolo 3.3 le regole di
ripartizione del delta nei quarti d'ora di ciascuna fascia.

## Calcolo del Delta fra totalizzatori

Ai fini del calcolo del delta non orario vengono applicate le seguenti
regole:

- Viene calcolato il delta di consumo non orario come differenza fra
  totalizzatori a partire dal dato di misura trasmesso al SII

- Il dato di misura usato ai fini del calcolo del calcolo del delta non
  orario:

  - È il dato riferito al totalizzatore, sia esso monorario che per
    fasce (EaM, EaFi[^3]);

  - Nel caso di flussi periodici, presenta il campo Validato="S";

  - Nel caso di flussi periodici può essere sia effettivo che stimato
    (TipoDato=S/E).

- in fase di rettifica della profilazione durante la sessione di SEM, i
  flussi periodici verranno integrati con i valori dei totalizzatori
  trasmessi all'interno dei flussi di rettifica trasmessi al SII (RNO,
  RNO2G ecc)

- Ai fini del calcolo del delta non orario di un determinato mese M, il
  SII considera tutti i flussi di misura ricevuti che abbiano date di
  riferimento dei totalizzatori pari all'ultimo giorno del mese, fatta
  eccezione per i flussi SMIS e per i flussi di attivazione e
  disattivazione;

- In caso di assenza di totalizzatori per l'ultimo giorno del mese M o
  per l'ultimo giorno del mese M-1, il SII non effettua il calcolo del
  consumo quartorario per quel mese.

- Vengono esclusi dai flussi ritenuti idonei ai fini del calcolo dei
  consumi i flussi di prestazione commerciale (VNO/VNO2G, SNM, SNM2G) ed
  alcuni flussi di misura riferiti alla prestazione tecnica (SM, SM2G,
  RT, RT2G, VP, VP2G) che non risultino essere una discontinuità in
  termini di letture rispetto ai flussi periodici, come invece avviene
  in caso di flusso SMIS.

- Il delta fra totalizzatori in caso di flusso SMIS è calcolato con le
  regole dedicate alla presenza del flusso SMIS descritte al paragrafo
  successivo.

- Qualora per uno stesso mese di competenza e per uno stesso POD fossero
  trasmessi più flussi con stessa data di riferimento del totalizzatore,
  [viene utilizzato ai fini del calcolo il flusso periodico [più
  recente]{.underline} pervenuto al SII]{.mark}. tale regola è da
  intendersi valida per il calcolo del consumo profilato durante la
  sessione periodica mensile.

- Durante la sessione di SEM, non vengono effettuate logiche sulle
  motivazioni presenti all'interno del flusso di rettifica, fatta
  eccezione per la rettifica con motivazione "3" che ha l'effetto di
  annullare il flusso di misura precedentemente trasmesso e le
  rettifiche con motivazione 4/5 (capitolo 3.2.2)

- In caso di presenza di flussi di Rettifica durante la sessione di
  conguaglio, questi hanno la prevalenza sui flussi Periodici a parità
  di data di riferimento del totalizzatore.

- Qualora il dato di consumo risulti un valore negativo in una o più
  fasce, il relativo consumo quartorario non può essere calcolato dal
  SII.

Al termine del calcolo del delta non orario fra totalizzatori il SII
definisce i valori:

$$Consumo\ F1 = \sum_{}^{}{Delta\ mensile\ in\ F1}$$

$$Consumo\ F2 = \sum_{}^{}{Delta\ mensile\ in\ F2}$$

$$Consumo\ F3 = \sum_{}^{}{Delta\ mensile\ in\ F3}$$

$$Consumo\ Mono = \sum_{}^{}{Delta\ mensile\ in\ M}$$

### Calcolo consumi in caso di SMIS

Nel caso di flusso SMIS presente nel mese M la procedura di calcolo
calcola il delta per la quota parte di mese antecedente allo SMIS ed il
delta per la quota parte di mese successiva allo SMIS. Questi due delta
vengono sommati per calcolare il Consumo mensile in Fi o nella fascia
Monoraria.

$$Delta\ PRE\ F1 = Totalizzatore\ smontaggio\ smis\ F1–\ totalizzatore\ F1\ precedente$$

$$Delta\ PRE\ F2 = Totalizzatore\ smontaggio\ smis\ F2–\ totalizzatore\ F2\ precedente$$

$$Delta\ PRE\ F3 = Totalizzatore\ smontaggio\ smis\ F3–\ totalizzatore\ F3\ precedente$$

$$Delta\ PRE\ Em = Totalizzatore\ smontaggio\ smis\ Em–\ totalizzatore\ Em\ precedente$$

$$Delta\ POST\ F1 = Totalizzatore\ \ F1–\ totalizzatore\ montaggio\ SMIS\ F1$$

$$Delta\ POST\ F2 = Totalizzatore\ \ F2–\ totalizzatore\ montaggio\ SMIS\ F2$$

$$Delta\ POST\ F3 = Totalizzatore\ F3–\ totalizzatore\ montaggio\ SMIS\ F3$$

$$Delta\ POST\ Em = Totalizzatore\ Em–\ totalizzatore\ SMIS\ Em$$

$$Consumo\ F1 = \sum_{}^{}{Delta\ mensile\ in\ F1} = Delta\ PRE\ F1 + Delta\ POST\ F1$$

$$Consumo\ F2 = \sum_{}^{}{Delta\ mensile\ in\ F2} = Delta\ PRE\ F2 + Delta\ POST\ F2$$

$$Consumo\ F3 = \sum_{}^{}{Delta\ mensile\ in\ F3} = Delta\ PRE\ F3 + Delta\ POST\ F3$$

$$Consumo\ Mono = \sum_{}^{}{Delta\ mensile\ in\ M} = Delta\ PRE\ Mono + Delta\ POST\ Mono$$

### Calcolo consumi in caso di rettifiche 4/5 o Forfait[^4]

Si specifica che per POD con trattamento ≠O, alternativamente alla
sezione "Misura" (all'interno della quale è presente il valore delle
segnanti del misuratore), può essere valorizzata la sezione "Consumo":
tale sezione viene valorizzata nel caso in cui GruppoMis = \"NO\" o se
GruppoMis = \"SI\" e Forfait = \"SI\".

In tal caso il Distributore comunica direttamente un valore di Consumo
ed una "Data di inizio periodo" a partire dalla quale è stato consumato
il valore dichiarato nel campo "Consumo", come riportato di seguito.

| **Consumo** | Sezione (alternativa a Misura) che contiene il dato di consumo del POD | Alternativo alla sezione Misura. Valorizzare se **GruppoMis = \"NO**\" o se **(GruppoMis = \"SI\" e Forfait = \"SI\")** (\*) | **DataInizioPeriodo** | Data inizio del periodo cui si riferiscono i consumi |
|-------------|------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|-----------------------|------------------------------------------------------|
|             |                                                                        |                                                                                                                              | **EaM**               | Consumo (Espresso in KWh)                            |

A partire dalla DataInizioPeriodo, il Consumo presente nel flusso viene
ripartito nei quarti d'ora del mese M.

Anche nel caso di rettifiche non orarie trasmesse con motivazione 4/5 o
per rettifiche per le quali Forfait=SI, viene applicata la stessa logica
di calcolo.

Per il calcolo del 1° giorno in cui non è presente il flusso di
rettifica con motivazione 4 o 5, all'interno del flusso di misura viene
nuovamente trasmesso il dato del totalizzatore in luogo del consumo
precedentemente dichiarato.

Per calcolare il consumo del primo giorno (o mese) in cui è nuovamente
presente il dato del totalizzatore, deve essere effettuato il delta tra
il totalizzatore di questo giorno e l'ultimo totalizzatore utile,
sommando a questo i consumi nei periodi ricostruiti, come da esempio
seguente:

| data_mis   | flx | Totalizzatore | kwh  |
|------------|-----|---------------|------|
| 31/01/2025 | PNO | T1            | 1100 |
| 28/02/2025 | PNO | T2            | 1300 |
| 31/03/2024 | PNO | T3            | 1500 |
| 30/04/2024 | RNO | Consumo1      | 150  |
| 31/05/2024 | RNO | Consumo2      | 160  |
| 30/06/2024 | PNO | T4            | 2000 |

[]{#_Toc195085267 .anchor}Tabella 3. Esempio flussi Ricostruzione
Consumi

| MESE       | Calcolo Consumo                 | Consumo |
|------------|---------------------------------|---------|
| febbraio   | T2-T1                           | 200     |
| marzo      | T3-T2                           | 200     |
| aprile     | Consumo1                        | 150     |
| maggio     | Consumo2                        | 160     |
| **giugno** | **(T4-(T3+Consumo1+Consumo2))** | **190** |

[]{#_Toc195085268 .anchor}Tabella 4. Calcolo Ricostruzione Consumi

## Ripartizione quartoraria del Delta fra totalizzatori

Una volta individuato il valore del delta non orario, il SII applica la
seguente modalità di profilazione quartoraria:

- l'energia elettrica prelevata in ciascun quarto d'ora in ciascun punto
  di misura per il quale è disponibile la misura dell'energia elettrica
  prelevata per fascia oraria, è pari al rapporto fra l'energia
  elettrica prelevata nel medesimo punto nella fascia oraria e nel mese
  in cui il quarto d'ora considerato appartiene (delta fra
  totalizzatori) e il numero di quarti d'ora della medesima fascia;

- l'energia elettrica prelevata in ciascun quarto d'ora in ciascun punto
  di misura per il quale è disponibile la misura dell'energia elettrica
  prelevata su base mensile, è pari al rapporto fra l'energia elettrica
  prelevata nel medesimo punto nel mese cui il quarto d'ora considerato
  appartiene (delta fra totalizzatori) e il numero di quarti d'ora del
  medesimo mese;

Per effettuare il rapporto di cui sopra, il SII si avvale della
suddivisione nelle tre fasce standard previste dall'Arera:

| F1  | 8 - 19.00 dei giorni lunedì - venerdì   |
|-----|-----------------------------------------|
| F2  | 7-8 e 19-23 dei giorni lunedì-venerdì   |
|     | 7-23 del sabato                         |
| F3  | 00-7 e 23-24 dei giorni lunedì-sabato   |
|     | tutte le ore per domenica e festivi[^5] |

fasce orarie - tabella di formattazione

[]{#_Toc195085269 .anchor}Tabella 5. Fasce Standard Arera

Partendo dalle fasce definite in tabella si riporta il numero dei quarti
d'ora che occupa ciascuna fascia in ciascun giorno del mese M:

- **F1**: quarti d'ora da \[QH33 ad QH76\] dal lunedì a venerdì

- **F2**: quarti d'ora da \[QH29 ad QH32\] e da \[QH77 ad QH92\] dal
  lunedì al venerdì

**F2**: quarti d'ora da \[QH29 ad QH92\] del sabato

- **F3**: quarti d'ora da \[QH1 a QH28\] e da \[QH93 a QH96\] da lunedì
  al sabato

**F3**: quarti d'ora da \[QH1 a QH100\] per i giorni di domenica e dei
festivi

Si riportano in formato tabellare i valori delle fasce ripartite nei
quarti d'ora

|                 | LUNEDÌ | MARTEDÌ | MERCOLEDÌ | GIOVEDÌ | VENERDÌ | SABATO | DOMENICA |
|-----------------|--------|---------|-----------|---------|---------|--------|----------|
| qh1... qh28     | F3     | F3      | F3        | F3      | F3      | F3     | F3       |
| qh29...qh32     | F2     | F2      | F2        | F2      | F2      | F2     | F3       |
| qh33...qh76     | F1     | F1      | F1        | F1      | F1      | F2     | F3       |
| qh77...qh92     | F2     | F2      | F2        | F2      | F2      | F2     | F3       |
| qh93...qh96[^6] | F3     | F3      | F3        | F3      | F3      | F3     | F3       |

Per calcolare il consumo profilato quartorario giornaliero di considera
come parametro da inserire al denominatore della formula del calcolo, il
numero dei quarti d'ora presenti per singola fascia e per mese.

<table>
<colgroup>
<col style="width: 10%" />
<col style="width: 40%" />
<col style="width: 49%" />
</colgroup>
<thead>
<tr class="header">
<th></th>
<th><p>N° quarti d’ora</p>
<p>nel mese in cui il POD è attivo</p></th>
<th><p>Esempio di mese di</p>
<p>31 giorni, con 4 domeniche e 4 sabati in cui il POD è sempre
attivo</p></th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>F1</td>
<td>NqhF1</td>
<td>1012</td>
</tr>
<tr class="even">
<td>F2</td>
<td>NqhF2</td>
<td>716</td>
</tr>
<tr class="odd">
<td>F3</td>
<td>NqhF3</td>
<td>1248</td>
</tr>
<tr class="even">
<td>Mono</td>
<td>NqhM</td>
<td>2976</td>
</tr>
</tbody>
</table>

[A questo punto è possibile calcolare il consumo del singolo quarto
d'ora per fascia di consumo:]{.underline}

- POD con trattamento F (nel caso di POD 2G si considera comunque il
  delta mensile dato dalla differenza tra lettura di fine mese e di
  inizio mese)

  - ${Consumo\ }_{QHF1} = \frac{\sum_{}^{}{Delta\ mensile\ in\ F1}}{NqhF1}$

> Dove

- Consumo QHF1 è il consumo del singolo quarto d'ora della fascia F1.
  > Tale consumo valorizza il consumo dei quarti d'ora da QH33 a QH76
  > per ogni singolo giorno del mese M diverso da sabato e domenica.

  - ${Consumo\ }_{QHF2} = \frac{\sum_{}^{}{Delta\ mensile\ in\ F2}}{NqhF2}$

- Consumo QHF2 è il consumo del singolo quarto d'ora della fascia F2.
  > Tale consumo valorizza il consumo dei quarti d'ora da QH29 a QH32 e
  > da QH77 a QH92 dei giorni del mese M che vanno dal lunedì al
  > venerdì, e per il giorno sabato valorizza anche il consumo dei
  > quarti d'ora che vanno da QH29 a QH92

  - ${Consumo\ giornaliero}_{QHF3} = \frac{\sum_{}^{}{Delta\ in\ F2}}{NqhF3}$

- Consumo QHF3 è il consumo del singolo quarto d'ora della fascia F3.
  > Tale consumo valorizza il consumo dei quarti d'ora da QH1 a QH28 dei
  > giorni che vanno dal lunedì al sabato, e valorizza il consumo dei
  > quarti d'ora da QH1 a QH96 della domenica.

- POD con trattamento M

  - ${Consumo\ }_{QHM} = \frac{\sum_{}^{}{Delta\ mensile\ in\ EM}}{NqhMono}$

- Consumo QHMono è il consumo del singolo quarto d'ora di ciascun quarto
  > d'ora del mese M.

[^1]: <https://siiportale.acquirenteunico.it/documents/812312/0/Specifiche_Tecniche_Standard_Misure_EE_ALLA_v.1.1>
    e
    <https://siiportale.acquirenteunico.it/documents/812312/0/Specifiche_Tecniche_Standard_SMIS_ALLA_v.1.1>

[^2]: I flussi di rettifica RNO, RNO2G, AVR, AVR2G, DSR, DSR2G verranno
    utilizzati nella sessione di rettifica prevista in concomitanza con
    la rettifica dell'aggregato MO e non nella sessione mensile (AC).
    Tale nota è da intendersi valida per ogni riferimento alla gestione
    delle rettifiche del presente documento.

[^3]: i=1,...,6

[^4]: Tale paragrafo è da intendersi valido per la profilazione dei
    consumi non orari durante la sessione di SEM

[^5]: Si considerano festivi: 1 gennaio; 6 gennaio; lunedì di Pasqua; 25
    aprile; 1 maggio; 2 giugno; 15 agosto; 1 novembre; 8 dicembre; 25
    dicembre; 26 dicembre.

[^6]: i quarti d'ora da 97 a 100 sono sempre pari a 0, tranne
    nell'ultima domenica di ottobre

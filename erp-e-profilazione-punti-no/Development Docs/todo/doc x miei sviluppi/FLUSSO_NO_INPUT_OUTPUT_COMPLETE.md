# 🔍 FLUSSO NO: TABELLE INPUT/OUTPUT E SPIEGAZIONE BLACKLIST

**Data:** 21 Ottobre 2025  
**Obiettivo:** Capire ESATTAMENTE quali tabelle legge il flusso NO, come si chiama, e cos'è il BLACKLIST

---

## 🚫 COSA SIGNIFICA "BLACKLISTATI" (IL CONCETTO CHIAVE)

### Righe 22-23 in CalcoloSegmentIConsumoNOTrasformation.scala:

```scala
private val windowErrorSegmentoSinistro  = Seq("SMIS_SMN","DS","DS2G")
private val windowErrorSegmentoDestro    = Seq("SMIS_MN","AV","AV2G")
```

### Cosa accade nel codice:

**Linea 139** (nel file letto):
```scala
.withColumn("invalid_flow",
  col(erpConsumptionNoSchema.tipo_flusso_sx.toString).isin(windowErrorSegmentoSinistro: _*).
    or(col(erpConsumptionNoSchema.tipo_flusso_dx.toString).isin(windowErrorSegmentoDestro: _*))
)
```

**Linea 177** (nel file letto):
```scala
.withColumn(erpConsumptionNoSchema.stato.toString,
  when(col("invalid_flow"), lit(ERR_MIS))  ← 🔴 SCATTA QUI!
  ...
)
```

### 🔴 Il BLACKLIST in pratica:

```
Se il segmento SX ha tipo_flusso in (SMIS_SMN, DS, DS2G) → col("invalid_flow") = TRUE
Se il segmento DX ha tipo_flusso in (SMIS_MN, AV, AV2G) → col("invalid_flow") = TRUE

Se invalid_flow = TRUE → stato = "ERR_MIS" (errore di flow)

✅ Segmenti con stato="OK" → vanno avanti nella pipeline
❌ Segmenti con stato="ERR_MIS" → vengono IGNORATI (non passano al passo dopo)
```

### 📊 Esempio pratico:

```
POD: 00123456789
Mese: 202410

Segmento SX: tipo_flusso="DS" (data_misura=30/09, totalizzatore=1000)
Segmento DX: tipo_flusso="PNO" (data_misura=31/10, totalizzatore=1100)

Nel codice:
  - Legge: tipo_flusso_sx = "DS" 
  - Controlla: "DS" è in windowErrorSegmentoSinistro? YES! 
  - Setta: invalid_flow = TRUE
  - Setta: stato = "ERR_MIS"
  - OUTPUT: Questo segmento viene MARCATO COME ERRORE

RISULTATO: Il delta DX-SX non viene calcolato!
         : il POD per ottobre rimane SENZA DATI in erp_aggregato_no
```

---

## 📋 LE 4 TABELLE INPUT (da `au` database)

| Nome in Config | Nome SQL | Descrizione | Database |
|---|---|---|---|
| `hive.table.au.flusso_misure_noaggr` | `au.flusso_misure_noaggr` | Flussi periodici NO: PNO, RNO, VNO, SMIS | **AU** |
| `hive.table.au.flussi_tecnici` | `au.flussi_tecnici` | Flussi tecnici: AV, AV2G, DS, DS2G | **AU** |
| `hive.table.au.flusso_misure_smis` | `au.flusso_misure_smis` | SMIS con smontaggio/montaggio | **AU** |
| `hive.table.tratt_pod.tratt_pod_all_annomese_partitioned` | `tratt_pod.tratt_pod_all_annomese_partitioned` | Dati distr, area, trattamento per POD/anno/mese | **TRATT_POD** |

---

## 🔗 PERCORSO HUE (per leggerle da interfaccia)

### Se accedi a Hue (http://hue-server:8888):

```
1. Seleziona DATABASE → au
   └─ Tabelle disponibili:
      ├─ flusso_misure_noaggr  ← Tabella 1: Input principale
      ├─ flussi_tecnici        ← Tabella 2: Flussi tecnici (attualmente blacklistati)
      └─ flusso_misure_smis    ← Tabella 3: SMIS smontaggio/montaggio

2. Seleziona DATABASE → tratt_pod
   └─ Tabelle disponibili:
      └─ tratt_pod_all_annomese_partitioned ← Tabella 4: Metadata POD
```

---

## 📊 SQL QUERY PER LEGGERE I DATI INPUT

### ✅ 1. FLUSSI PERIODICI NO (PNO, RNO, VNO, SMIS)

```sql
-- Legge i flussi periodici PNO, RNO e raccoglie i segmenti per calcolo delta
SELECT 
  pod,
  piva_distributore,
  area,
  data_misura,
  tipo_flusso,
  trattamento,
  eam,
  eaf1,
  eaf2,
  eaf3,
  perdita,
  ka,
  tipodato_e,
  tipodato_s,
  validato,
  time_stamp,
  anno,
  mese
FROM au.flusso_misure_noaggr
WHERE anno = 2024
  AND mese = 10
  AND tipo_flusso IN ('PNO', 'RNO', 'PNO2G', 'RNO2G', 'VNO', 'VNO2G', 'SNM', 'SNM2G')
  AND validato = 'S'
LIMIT 100;

-- Per vedere i dati stimati che VENGONO SCARTATI (BLOCCO 3):
SELECT 
  pod,
  data_misura,
  tipo_flusso,
  tipodato_e,  ← 0 = Stimato (S), 1 = Effettivo (E)
  tipodato_s,
  eam,
  COUNT(*) as num_records
FROM au.flusso_misure_noaggr
WHERE anno = 2024
  AND mese = 10
  AND tipodato_e = '0'  ← QUESTI VENGONO SCARTATI!
  AND tipo_flusso IN ('PNO', 'RNO')
GROUP BY pod, data_misura, tipo_flusso, tipodato_e, tipodato_s
ORDER BY COUNT(*) DESC
LIMIT 50;
```

---

### 🚫 2. FLUSSI TECNICI (AV, DS - ATTUALMENTE BLACKLISTATI)

```sql
-- Legge i flussi tecnici
-- ⚠️ QUESTI VENGONO ELABORATI MA MARCATI COME ERRORE "ERR_MIS" nella FASE 2!
SELECT 
  pod,
  piva_distributore,
  area,
  data_misura,
  tipo_flusso,       ← AV, AV2G, DS, DS2G
  trattamento,
  eam,
  eaf1,
  eaf2,
  eaf3,
  perdita,
  ka,
  validato,
  time_stamp,
  anno,
  mese
FROM au.flussi_tecnici
WHERE anno = 2024
  AND mese = 10
  AND tipo_flusso IN ('AV', 'AV2G', 'DS', 'DS2G')
  AND validato = 'S'
LIMIT 100;

-- Per vedere QUANTI dati AV/DS sono stati scartati:
SELECT 
  tipo_flusso,
  COUNT(*) as num_records,
  COUNT(DISTINCT pod) as num_pod
FROM au.flussi_tecnici
WHERE anno = 2024
  AND mese = 10
  AND tipo_flusso IN ('AV', 'AV2G', 'DS', 'DS2G')
GROUP BY tipo_flusso;
```

---

### 📏 3. SMIS (Smontaggio/Montaggio Misuratore)

```sql
-- SMIS con data SMONTAGGIO (quando viene tolto il misuratore vecchio)
SELECT 
  pod,
  piva_distributore,
  area,
  data_misura_smn,   ← Data smontaggio
  eam_smn,
  eaf1_smn,
  eaf2_smn,
  eaf3_smn,
  tipo_dato_smn,      ← E=Effettivo, S=Stimato
  anno_dtms,
  mese_dtms,
  perditatens_mn,
  ka_mn
FROM au.flusso_misure_smis
WHERE anno_dtms = 2024
  AND mese_dtms = 10
LIMIT 50;

-- Per vedere i SMIS in coppia (smontaggio/montaggio dello stesso POD):
SELECT 
  pod,
  COUNT(*) as num_smis_records
FROM au.flusso_misure_smis
WHERE anno_dtms = 2024
  AND mese_dtms = 10
GROUP BY pod
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 20;
```

---

### 🏷️ 4. TRATT_POD (Metadata: area, distributore, trattamento)

```sql
-- Legge i dati di trattamento del POD per abbinamento
SELECT 
  pod14,                    ← POD a 14 cifre
  piva_distributore,
  area,
  trattamento,             ← M=Monorario, F=Fasciato, C=Complesso
  is_t_trattamento,        ← 1=sì, 0=no
  anno,
  mese
FROM tratt_pod.tratt_pod_all_annomese_partitioned
WHERE anno = 2024
  AND mese = 10
  AND piva_distributore = '00402780217'  ← Esempio: Distributore specifico
LIMIT 50;
```

---

## 📤 LE 4 TABELLE OUTPUT (generate dalla pipeline)

| Fase | Tabella Output | Database | Schema | Descrizione |
|---|---|---|---|---|
| 1 | `erp_validated_mis_no` | erp | `erpValidatedMisNoSchema` | Totalizzatori grezzi da flussi INPUT |
| 2 | `erp_consumption_no` | erp | `erpConsumptionNoSchema` | Delta tra SX e DX (con stato=OK/ERR_MIS) |
| 3 | `erp_daily_no` | erp | `erpDailyNoSchema` | 100 quarti per ogni giorno |
| 4 | `erp_aggregato_no` | erp | (implicitly) | **TAVOLA FINALE**: Quarti aggregati per POD/giorno/area |

---

## 🔍 SQL PER VERIFICARE OUTPUT

### Legge FASE 2 (vedi quali segmenti hanno stato="ERR_MIS"):

```sql
-- Vedi quanti segmenti sono ERRORE per i flussi blacklistati:
SELECT 
  tipo_flusso_sx,
  tipo_flusso_dx,
  stato,
  COUNT(*) as num_segmenti
FROM erp.erp_consumption_no
WHERE anno = 2024
  AND mese = 10
  AND (tipo_flusso_sx IN ('DS', 'DS2G', 'SMIS_SMN')
       OR tipo_flusso_dx IN ('AV', 'AV2G', 'SMIS_MN'))
GROUP BY tipo_flusso_sx, tipo_flusso_dx, stato
ORDER BY COUNT(*) DESC;

-- Risultato atteso:
-- tipo_flusso_sx | tipo_flusso_dx | stato   | num_segmenti
-- DS             | PNO            | ERR_MIS | 1500
-- AV             | RNO            | ERR_MIS | 2000
-- SMIS_SMN       | SMIS_MN        | ERR_MIS | 800
```

### Legge FASE 4 (vedi i dati finali):

```sql
-- Vedi i quarti aggregati finali per un POD specifico
SELECT 
  pod,
  anno,
  mese,
  giorno,
  area,
  piva_distr,
  rag_soc_distr,
  q1, q2, q3, q4, q5,  -- First 5 quarters
  q96, q97, q98, q99, q100  -- Last 5 quarters
FROM erp.erp_aggregato_no
WHERE anno = 2024
  AND mese = 10
  AND pod = '00123456789123456'  ← POD specifico
ORDER BY giorno;

-- Risultato: ogni riga = 1 POD, 1 giorno, con 100 colonne di quarti
```

---

## 🎯 AZIONI IMMEDIATE

### Per FISSARE il BLACKLIST:

**File:** `CalcoloSegmentIConsumoNOTrasformation.scala`, Righe 22-23

**Cambiare:**
```scala
private val windowErrorSegmentoSinistro  = Seq("SMIS_SMN","DS","DS2G")
private val windowErrorSegmentoDestro    = Seq("SMIS_MN","AV","AV2G")
```

**In:**
```scala
private val windowErrorSegmentoSinistro  = Seq("SMIS_SMN")  ← Solo SMIS vero errore
private val windowErrorSegmentoDestro    = Seq("SMIS_MN")   ← Solo SMIS vero errore
```

### Per FISSARE il TipoDato filter:

**File:** `CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.scala`, Righe 56, 105, 137

**Cambiare:**
```scala
.filter(col(flussoMisureNoAggrSchema.tipodato_e) === Constants.flussiPeriodiciMisuraEffettivaStimataTipoDatoE1)
```

**In:**
```scala
.filter(col(flussoMisureNoAggrSchema.tipodato_e).isin("1", "0"))  ← Accetta sia effettivo che stimato
```

---

## 📝 DIAGRAMMA: Cosa accade se NON fissi?

```
INPUT (AU):
  └─ POD ABC, mese 10, tipo_flusso="DS"  ← Dato in AU
     │
     ├─ FASE 1 (Ingestion): ✅ ELABORATO
     │  └─ Output: erp_validated_mis_no con tipo_flusso="DS"
     │
     ├─ FASE 2 (Consumo): ❌ MARCATO ERRORE
     │  └─ Check: tipo_flusso="DS" è in windowErrorSegmentoSinistro? YES!
     │  └─ Output: erp_consumption_no con stato="ERR_MIS"
     │
     ├─ FASE 3 (Profiling): ⏭️ SALTATO
     │  └─ Non elabora segmenti con stato="ERR_MIS"
     │  └─ Output: erp_daily_no NON contiene quarti per POD ABC
     │
     └─ FASE 4 (Aggregazione): ❌ VUOTO
        └─ Output: erp_aggregato_no NON ha record per POD ABC, mese 10
        └─ RESU​LTATO: Dati PERSI completamente!
```

---

## 📞 NEXT STEPS

1. ✅ Capito COSA è il BLACKLIST? **SÌ** - Sono flussi marcati come ERR_MIS prima di essere elaborati
2. ✅ Capito dove sono le tabelle input? **SÌ** - Database `au`, `tratt_pod` (vedi percorsi Hue sopra)
3. ✅ Capito i SQL per leggere i dati? **SÌ** - Vedi 4 query sopra
4. ⏳ **PROSSIMO:** Runna le query SQL e verifica quanti dati AV/DS stai perdendo in agosto/settembre/ottobre

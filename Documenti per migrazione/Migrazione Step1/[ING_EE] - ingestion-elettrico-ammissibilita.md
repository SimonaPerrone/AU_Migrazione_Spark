# STEP 1: MIGRAZIONE ARCHITETTURALE ingestion-elettrico-ammissibilita

## YARN+NFS → YARN(decomp)+CDE(calcolo) + HDFS Gateway + Kerberos

---

## MODIFICHE QUICK VIEW

| Componente | PRIMA | DOPO | File |
|---|---|---|---|
| **UnzipDriver** | YARN + NFS | YARN + HDFS Gateway + Kerberos | `spark-submit-ingestion-elettrico-ammissibilita-unzip.sh` |
| **FlowDriver** | YARN + NFS | CDE + HDFS + Kerberos | `spark-submit-ingestion-elettrico-ammissibilita-ingestion.sh` |
| **Autenticazione** | — | `kinit` (Kerberos) | Aggiungi agli script |
| **Path Input** | `/mnt/isilonshare_ingestion-elettrico-ammissibilita` | `/mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/input` | `config.properties` |
| **Path Temp** | `/mnt/isilonshare1/ingestion-elettrico-ammissibilita_INJ_STD/...` | `/mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/temp` | `config.properties` |
| **Storage** | Isilon NFS | HDFS via Gateway (esposto NFS) | Infrastructure |

---

## A. SCRIPT DA MODIFICARE

### A1. `spark-submit-ingestion-elettrico-ammissibilita-unzip.sh` - YARN Decompression

```bash
#!/bin/bash
set -e

# KERBEROS AUTHENTICATION (CDP 7.1.9)
kinit -kt /etc/security/keytabs/airflow.keytab airflow@REALM.COM
if [ $? -ne 0 ]; then
  echo "ERROR: Kerberos authentication failed" >&2
  exit 1
fi

# RESTO DEL COMANDO RIMANE UGUALE
spark2-submit \
    --class it.au.ingestion-elettrico-ammissibilita.driver.UnzipDriver \
    --master yarn \
    ... (resto invariato)
A2. spark-submit-ingestion-elettrico-ammissibilita-ingestion.sh - CDE Calcolo
bash


#!/bin/bash
set -e

# KERBEROS AUTHENTICATION (CDP 7.1.9)
kinit -kt /etc/security/keytabs/airflow.keytab airflow@REALM.COM
if [ $? -ne 0 ]; then
  echo "ERROR: Kerberos authentication failed" >&2
  exit 1
fi

# OPZIONE 1: Mantieni YARN (per ora)
spark2-submit --master yarn \
    --class it.au.ingestion-elettrico-ammissibilita.driver.FlowDriver \
    ... (resto invariato)

# OPZIONE 2: Migra a CDE (quando pronto)
# spark2-submit --master kubernetes \
#     --conf spark.kubernetes.authenticate.cdeUser=airflow \
#     --class it.au.ingestion-elettrico-ammissibilita.driver.FlowDriver \
#     ... (resto invariato)
B. CONFIGURAZIONE PROPERTIES
B1. config.properties - PATH UPDATES
properties


# AGGIORNA TUTTI I PERCORSI:

# Vecchio percorso root
# root.path=/mnt

# NUOVO percorso root
root.path=/mnt/hdfs-gateway

# Input paths
unzip.input.path=/mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/input
# PRIMA: /mnt/isilonshare_ingestion-elettrico-ammissibilita

# Temp paths
temp.path=/mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/temp
# PRIMA: /mnt/isilonshare1/ingestion-elettrico-ammissibilita_INJ_STD/isilonshare_ingestion-elettrico-ammissibilita

temp.path.old=/mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/temp_old
# PRIMA: /mnt/isilonshare1/ingestion-elettrico-ammissibilita_INJ/isilonshare_ingestion-elettrico-ammissibilita

# XSD e config
xsd.path=/mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/xsd
# PRIMA: /mnt/isilonshare1/XSD_ingestion-elettrico-ammissibilita_STANDARD

# Validation/reports
ammissibilita.standard.path=/mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/ammissibilita
# PRIMA: /mnt/isilonshare1/TMG_SYNC_TMP1

# Deploy
deploy.path=/mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/deploy
# PRIMA: /mnt/isilonshare1/Software/ingestion-elettrico-ammissibilita/STANDARD
B2. application.conf - VERIFY/UPDATE
properties


# Verifica e aggiorna se presenti:

rootPath=/mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/ingestion
# PRIMA: /mnt/isilonshare1/ingestion-elettrico-ammissibilita_INJ_STD/...

tempRootPath=/mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/temp
# PRIMA: /mnt/isilonshare1/ingestion-elettrico-ammissibilita_INJ_STD/...
C. FILE DI CONFIGURAZIONE KERBEROS (NEW)
C1. Creare: kerberos.conf
properties


# File di riferimento per Kerberos
kerberos.keytab=/etc/security/keytabs/airflow.keytab
kerberos.principal=airflow@REALM.COM
kerberos.realm=REALM.COM
D. STRUTTURA DIRECTORY HDFS GATEWAY
Deve essere creata nel sistema:



/mnt/hdfs-gateway/
├── ingestion-elettrico-ammissibilita/
│   ├── input/          ← File input originali (ZIP/CSV/etc)
│   ├── temp/           ← File decompressed/temporanei
│   ├── temp_old/       ← Backup temp
│   ├── ammissibilita/  ← Validation reports
│   ├── xsd/            ← Schema XSD
│   └── deploy/         ← JAR e config
E. COMANDI DI TEST
Esegui in ordine per validare setup:




Test	Comando	Expected
Kerberos	kinit -kt /etc/security/keytabs/airflow.keytab airflow@REALM.COM && klist -s	Exit code 0
Gateway Access	ls /mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/input/	Directory listing OK
Write Access	touch /mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/temp/test.txt && rm test.txt	File created/deleted
UnzipDriver	bash spark-submit-ingestion-elettrico-ammissibilita-unzip.sh --input=sample.zip	Job SUCCEEDED
FlowDriver	bash spark-submit-ingestion-elettrico-ammissibilita-ingestion.sh --input=unzipped/	Job SUCCEEDED
F. CHECKLIST IMPLEMENTAZIONE
Segui in ordine:



INFRA
  ☐ Keytab /etc/security/keytabs/airflow.keytab creato + permessi OK
  ☐ Mount /mnt/hdfs-gateway disponibile e accessibile
  ☐ Directory structure creata (vedi Sezione D)

CODE
  ☐ Modifica spark-submit-ingestion-elettrico-ammissibilita-unzip.sh con kinit
  ☐ Modifica spark-submit-ingestion-elettrico-ammissibilita-ingestion.sh con kinit
  ☐ Update config.properties con nuovi path (Sezione B1)
  ☐ Update application.conf con nuovi path se presente (Sezione B2)
  ☐ Creare kerberos.conf (Sezione C1)

BUILD
  ☐ mvn clean install -DskipTests
  ☐ JAR build completato senza errori

TEST (in ordine della Sezione E)
  ☐ Kerberos auth test PASSED
  ☐ Gateway access test PASSED
  ☐ Write access test PASSED
  ☐ UnzipDriver test PASSED
  ☐ FlowDriver test PASSED

VALIDATION
  ☐ Output identico a versione precedente
  ☐ Performance accettabili
  ☐ Log comprensibili e completi
G. AUTENTICAZIONE SECONDO CDP 7.1.9
Per YARN (UnzipDriver)
✅ Usa kinit con keytab file (CDP 7.1.9 default)
✅ Keytab ubicazione standard: /etc/security/keytabs/
✅ Principal format: user@REALM.COM
✅ Comando: kinit -kt /etc/security/keytabs/airflow.keytab airflow@REALM.COM

Per CDE (FlowDriver - se migrato dopo)
✅ Usa CDE credentials file o Kerberos
✅ Se CDE: scarica credentials.json da CDE UI
✅ Se rimane YARN: usa kinit come UnzipDriver

H. MAPPING PROCEDURA COMPLETA
Fase di implementazione step-by-step:




Fase	Componente	Azione	File	Status
1	Infrastructure	Verificare keytab + mount	/etc/security/keytabs/	☐
2	Script Unzip	Add kinit + test	spark-submit-ingestion-elettrico-ammissibilita-unzip.sh	☐
3	Script Flow	Add kinit/CDE + test	spark-submit-ingestion-elettrico-ammissibilita-ingestion.sh	☐
4	Config Props	Update path input/temp	config.properties	☐
5	Config App	Verify path se presente	application.conf	☐
6	Kerberos	Creare file config	kerberos.conf	☐
7	Build	Rebuild JAR	mvn clean install	☐
8	Test	End-to-end flow	CLI commands Sezione E	☐
9	Validation	Confrontare risultati	Output reports	☐
I. NOTE IMPORTANTI
Path mapping: ogni occorrenza di /mnt/isilonshare* → /mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/*
Kerberos: kinit DEVE eseguire subito all'inizio dello script, prima di qualunque operazione Spark
YARN vs CDE: puoi mantenere YARN per ora (Opzione 1 in A2), migrazione CDE è fase successiva
Keytab permissions: ls -la /etc/security/keytabs/airflow.keytab → deve essere readable da utente airflow
Directory ownership: /mnt/hdfs-gateway/ingestion-elettrico-ammissibilita/* → deve essere writable da airflow
Config consolidation: se hai altri file di config, aggiorna anche quelli con i nuovi path
J. TROUBLESHOOTING



Errore	Causa	Soluzione
kinit: Cannot open keyfile	Keytab non trovato o permessi errati	Verifica path keytab, esegui chmod 600 airflow.keytab
Permission denied: /mnt/hdfs-gateway_	Directory non accessible	Verifica mount + ownership directory
Unresolved host_	Network/DNS issue	Verifica /etc/hosts + ping REALM host
Job failed - file not found	Path ancora vecchio	Verifica config.properties aggiornato
Spark connection timeout	Kerberos token scaduto	Aggiungi kinit -R prima di spark-submit per rinnovare

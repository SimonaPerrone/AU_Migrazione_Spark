# STEP 1: MIGRAZIONE ARCHITETTURALE ingestion-misure-gas-unico
## YARN+NFS → YARN(decomp)+CDE(calcolo) + HDFS Gateway + Kerberos

---

## MODIFICHE QUICK VIEW

| Componente | PRIMA | DOPO | File |
|---|---|---|---|
| **UnzipDriver** | YARN + NFS | YARN + HDFS Gateway + Kerberos | `spark-submit-gas-unzip.sh` |
| **FlowDriver** | YARN + NFS | CDE + HDFS + Kerberos | `spark-submit-gas-ingestion.sh` |
| **Autenticazione** | — | `kinit` (Kerberos) | Aggiungi agli script |
| **Path Input** | `/mnt/isilonshare_gas` | `/mnt/hdfs-gateway/ingestion/input` | `config.properties` |
| **Path Temp** | `/mnt/isilonshare1/GAS_INJ_STD/...` | `/mnt/hdfs-gateway/ingestion/temp` | `config.properties` |
| **Storage** | Isilon NFS | HDFS via Gateway (esposto NFS) | Infrastructure |

---

## A. SCRIPT DA MODIFICARE

### A1. `spark-submit-gas-unzip.sh` - YARN Decompression

bash
# ADD ALL'INIZIO:
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
    --class it.au.misure.ingestionMisureGasUnico.driver.UnzipDriver \
    --master yarn \
    ... (resto invariato)
	
## A2. spark-submit-gas-ingestion.sh - CDE Calcolo

bash
# ADD ALL'INIZIO:
#!/bin/bash
set -e

# KERBEROS AUTHENTICATION (CDP 7.1.9)
kinit -kt /etc/security/keytabs/airflow.keytab airflow@REALM.COM
if [ $? -ne 0 ]; then
  echo "ERROR: Kerberos authentication failed" >&2
  exit 1
fi

# MODIFICARE master YARN → CDE (se in CDE)
# OPPURE mantenerlo YARN se rimane su YARN per ora

# Se mantieni YARN:
spark2-submit --master yarn ...

# Se vai su CDE:
spark2-submit --master kubernetes \
    --conf spark.kubernetes.authenticate.cdeUser=airflow \
    ...

B. CONFIGURAZIONE PROPERTIES
B1. config.properties - PATH UPDATES
| Property | VECCHIO | NUOVO |
| --- | --- | --- |
| root.path | /mnt | /mnt/hdfs-gateway |
| unzip.input.path | /mnt/isilonshare_gas | /mnt/hdfs-gateway/ingestion/input |
| temp.path | /mnt/isilonshare1/GAS_INJ_STD/isilonshare_gas | /mnt/hdfs-gateway/ingestion/temp |
| temp.path.old | /mnt/isilonshare1/GAS_INJ/isilonshare_gas | /mnt/hdfs-gateway/ingestion/temp_old |
| xsd.path | /mnt/isilonshare1/XSD_GAS_STANDARD | /mnt/hdfs-gateway/xsd |
| ammissibilita.standard.path | /mnt/isilonshare1/TMG_SYNC_TMP1 | /mnt/hdfs-gateway/ammissibilita |
| deploy.path | /mnt/isilonshare1/Software/GAS/STANDARD | /mnt/hdfs-gateway/deploy |

B2. application.conf - VERIFY/UPDATE
| Property | VECCHIO | NUOVO |
| --- | --- | --- |
| rootPath | /mnt/isilonshare1/... | /mnt/hdfs-gateway/ingestion |
| tempRootPath | /mnt/isilonshare1/GAS_INJ_STD/... | /mnt/hdfs-gateway/ingestion/temp |

C. FILE DI CONFIGURAZIONE KERBEROS (NEW)
C1. Creare: kerberos.conf

# File di riferimento per script
kerberos.keytab=/etc/security/keytabs/airflow.keytab
kerberos.principal=airflow@REALM.COM
kerberos.realm=REALM.COM

D. STRUTTURA DIRECTORY HDFS GATEWAY
/mnt/hdfs-gateway/
├── ingestion/
│   ├── input/          ← File ZIP originali
│   ├── temp/           ← File decompressed
│   └── temp_old/       ← Backup temp
├── ammissibilita/      ← Validation reports
├── xsd/                ← Schema XSD
└── deploy/             ← JAR e config

E. COMANDI DI TEST
| Test | Comando | Expected |
| --- | --- | --- |
| Kerberos | kinit -kt /etc/security/keytabs/airflow.keytab airflow@REALM.COM && klist -s | Exit code 0 |
| Gateway | ls /mnt/hdfs-gateway/ingestion/input/ | Directory listing OK |
| Write Access | touch /mnt/hdfs-gateway/ingestion/temp/test.txt | File created |
| UnzipDriver | bash spark-submit-gas-unzip.sh --input=sample.zip | Job completed |
| FlowDriver | bash spark-submit-gas-ingestion.sh --input=unzipped/ | Job completed |

F. CHECKLIST IMPLEMENTAZIONE
| Step | Item | ✓ |
| --- | --- | --- |
| INFRA | Keytab /etc/security/keytabs/airflow.keytab creato | ☐ |
| INFRA | Mount /mnt/hdfs-gateway disponibile | ☐ |
| CODE | Modifica spark-submit-gas-unzip.sh con kinit | ☐ |
| CODE | Modifica spark-submit-gas-ingestion.sh con kinit | ☐ |
| CODE | Update config.properties path | ☐ |
| CODE | Update application.conf path | ☐ |
| CODE | Creare kerberos.conf | ☐ |
| BUILD | mvn clean install -DskipTests | ☐ |
| TEST | Kerberos auth test PASSED | ☐ |
| TEST | Gateway access test PASSED | ☐ |
| TEST | UnzipDriver PASSED | ☐ |
| TEST | FlowDriver PASSED | ☐ |

G. AUTENTICAZIONE SECONDO CDP 7.1.9
Per YARN (UnzipDriver):
✅ Usa kinit con keytab file (CDP 7.1.9 default)
✅ Keytab ubicazione standard: /etc/security/keytabs/
✅ Principal format: user@REALM.COM
Per CDE (FlowDriver - se migrato):
✅ Usa CDE credentials file o Kerberos
✅ Se CDE: scarica credentials.json da CDE UI
✅ Se rimane YARN: usa kinit come UnzipDriver

H. MAPPING PROCEDURA
| Fase | Componente | Azione | File |
| --- | --- | --- | --- |
| 1 | Infrastructure | Verificare keytab + mount | CLI |
| 2 | Script Unzip | Add kinit + test | spark-submit-gas-unzip.sh |
| 3 | Script Flow | Add kinit/CDE + test | spark-submit-gas-ingestion.sh |
| 4 | Config | Update path | config.properties |
| 5 | Config | Verify path | application.conf |
| 6 | Build | Rebuild JAR | mvn clean install |
| 7 | Test | End-to-end flow | CLI commands |



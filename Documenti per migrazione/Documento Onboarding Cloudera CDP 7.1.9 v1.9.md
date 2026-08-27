![Confronta le offerte luce e gas e risparmia](data:image/jpeg;base64...)

Sommario

[1 Revisione ed approvazione 2](#_Toc1505139123)

[2 Scopo del documento 3](#_Toc674962455)

[3 Cloudera Data Platform 4](#_Toc1768339267)

[3.1 Cloudera Data Services 4](#_Toc1167272096)

[3.2 Cloudera Flow Management 5](#_Toc35154366)

[3.3 Nuovi servizi presenti 5](#_Toc1086410190)

[3.3.1 Ranger 6](#_Toc727140105)

[3.3.2 Knox 7](#_Toc43317029)

[3.3.3 Atlas 7](#_Toc1299167440)

[4 Sicurezza 7](#_Toc1626826609)

[4.1 Autenticazione 8](#_Toc1125587822)

[4.1.1 Autenticazione Kerberos 8](#_Toc1149390862)

[4.2 TLS 9](#_Toc1647551832)

[5 Infrastruttura 10](#_Toc292039836)

[5.1 Versione delle Componenti 11](#_Toc500790460)

[6 Ambiente di Collaudo 11](#_Toc739126099)

[6.1 Cluster e Servizi 12](#_Toc2022992779)

[6.1.1 Cloudera Manager 12](#_Toc1362433864)

[6.1.2 Host del Cluster 12](#_Toc59560085)

[6.1.3 Nodi Master 12](#_Toc1518219529)

[6.1.4 Nodi NiFi 12](#_Toc2095613664)

[6.1.5 Nodi Utility 12](#_Toc267494708)

[6.1.6 Nodi Worker 13](#_Toc2048973910)

[6.2 Servizi Installati 13](#_Toc515189694)

[6.3 HDFS 14](#_Toc1678272941)

[6.4 Framework Presenti 15](#_Toc229990531)

[6.5 Endpoint Principali 15](#_Toc883804048)

[6.5.1 Servizi 16](#_Toc45769971)

[6.5.2 Web UI 18](#_Toc337912144)

[6.6 NFS – HDFS 19](#_Toc289283726)

[7 Ambiente di Produzione 20](#_Toc1367391259)

[7.1 Cluster e Servizi 21](#_Toc2110827297)

[7.1.1 Cloudera Manager 21](#_Toc1761761058)

[7.1.2 Host del Cluster 21](#_Toc560024744)

[7.1.3 Nodi Master 22](#_Toc540043295)

[7.1.4 Nodi Utility 22](#_Toc743756981)

[7.1.5 Nodi NiFi 22](#_Toc1731660855)

[7.1.6 Nodi Worker 22](#_Toc1252777799)

[7.2 Servizi installati 22](#_Toc443698331)

[7.3 HDFS 23](#_Toc1480528947)

[7.4 Framework Presenti 24](#_Toc1751032156)

[7.5 Endpoint Principali 24](#_Toc163213946)

[7.5.1 Servizi 25](#_Toc794457496)

[7.5.2 Web UI 27](#_Toc1180404116)

[8 Cloudera Data Services 28](#_Toc1510050968)

[8.1 Host del Cluster 29](#_Toc922628477)

[8.2 Nodi ECS Server 29](#_Toc109410895)

[8.3 Nodi ECS Agent 29](#_Toc736347465)

[8.4 Runtime Compatibility 29](#_Toc1210092871)

[8.5 Environments 30](#_Toc2021217214)

[8.6 Framework Presenti 31](#_Toc8768684)

[8.6.1 Collaudo 32](#_Toc283738223)

[8.6.2 Produzione 32](#_Toc1302836857)

[8.7 Note per l’esecuzione dei job su YARN 32](#_Toc703363529)

[8.8 Note per l’esecuzione dei job su Data Services 33](#_Toc611119989)

[8.8.1 Collaudo 33](#_Toc1767943559)

[8.8.2 Produzione 35](#_Toc671543885)

[8.8.3 Credenziali 35](#_Toc466890935)

[8.8.4 CDE\_CONFIG\_PROFILE 35](#_Toc1809119714)

[8.8.5 Files 35](#_Toc282571130)

[8.8.6 Configurazioni non supportate 36](#_Toc539061014)

[8.9 Gestione Job e log Spark via CLI 36](#_Toc2128540187)

[8.9.1 Comandi CDE CLI 36](#_Toc1493004748)

[8.9.2 Visibilità dei log dall’interfaccia CDE 43](#_Toc1705339609)

[8.9.3 Riferimenti 43](#_Toc500522248)

[8.10 Installare librerie specifiche in CDE 44](#_Toc1356828378)

[8.10.1 Requisiti 44](#_Toc368264409)

[8.10.2 Creazione Virtual Environment Python 44](#_Toc124854478)

[8.10.3 Eseguire un spark submit con il Virtual Environment Python creato 45](#_Toc1658870035)

[8.10.4 Riferimenti 45](#_Toc1067479759)

[8.11 Immagini Docker Custom 45](#_Toc1651014234)

[8.11.1 Esecuzione di un job Spark con immagine custom 46](#_Toc35379407)

[9 NiFi 48](#_Toc1384656852)

[9.1 Processori Hive 49](#_Toc849629122)

[9.2 GenerateFlowFile 50](#_Toc536649488)

[10 Sqoop 51](#_Toc1703545434)

[10.1 Direct mode 51](#_Toc1827915500)

[11 Appendice 51](#_Toc551639677)

[11.1 Verifiche di connettività 52](#_Toc257407147)

1. Revisione ed approvazione

|  |  |  |  |
| --- | --- | --- | --- |
| Data | Autore | Versione | Stato |
| 23/04/2026 | Olidata | 1.0 | Bozza |
| 07/05/2026 | Olidata | 1.2 | Prima Revisione |
| 15/05/2026 | Olidata | 1.3 | Seconda Revisione |
| 18/05/2026 | Olidata | 1.4 | Aggiornati paragrafi 6.6 e 8.8.1.1 |
| 19/05/2026 | Olidata | 1.5 | Aggiunto paragrafo 8.4 |
| 29/05/2026 | Olidata | 1.6 | Aggiunto paragrafo 10 |
| 09/06/2026 | Olidata | 1.7 | Aggiunti paragrafi 8.9 e 8.10 |
| 13/07/2026 | Olidata | 1.9 | Aggiunti i paragrafi 8.7 e 8.9.2 |

1. Scopo del documento

**Cloudera CDP Private Cloud Base** rappresenta una soluzione avanzata per la gestione e l'elaborazione di grandi volumi di dati, progettata per soddisfare requisiti di scalabilità, affidabilità e performance richiesti dalle infrastrutture moderne.

Questo documento fornisce una guida all'utilizzo efficace del cluster, includendo:

* Una panoramica sui servizi disponibili e le relative funzionalità.
* Le modalità di accesso agli endpoint principali.
* Le procedure per connettersi al cluster.
* Le configurazioni necessarie per sfruttare al meglio le potenzialità offerte.

Saranno inoltre descritte le caratteristiche dei principali servizi, con particolare attenzione alle integrazioni, alle capacità di elaborazione e alle funzionalità analitiche. L'obiettivo è garantire una comprensione approfondita dell'ambiente e delle risorse disponibili, agevolandone l'adozione e l'uso nel rispetto delle migliori pratiche operative.

1. Cloudera Data Platform

Gli ambienti di **Collaudo** e **Produzione** di **Acquirente Unico** utilizzano la distribuzione **Cloudera CDP Private Cloud Base 7.1.9 SP1**.

A differenza del precedente cluster, nel quale il file system HDFS era simulato tramite tecnologia Dell PowerScale (Isilon), nell’attuale il servizio è installato nativamente. Questo significa che i dati sono conservati nei nodi del cluster di tipologia “Worker Node” e le operazioni di scrittura/lettura risultano più efficienti grazie alla prossimità (Data Locality), eliminando così la precedente latenza dovuta agli accessi via rete.

Un’ulteriore differenza consiste nel fattore di replica offerto dal servizio nativo HDFS, che nel deployment attuale è impostato a 2x, garantendo una migliore resilienza del dato. Deve essere quindi tenuto presente che il caricamento di un file sul cluster, occuperà il doppio dello spazio, rispetto alle sue dimensioni originali.

Insieme al cluster base, saranno installati anche i seguenti servizi:

* Cloudera Data Services
* Cloudera Flow Management
  1. Cloudera Data Services

Cloudera Data Services introduce un modello di erogazione dei servizi basato su container, superando il paradigma tradizionale di YARN. Questo approccio consente una maggiore elasticità, un migliore isolamento dei carichi di lavoro e una gestione più efficiente delle risorse, permettendo di scalare e aggiornare i singoli servizi in modo indipendente e con un impatto ridotto sull’intero cluster.

Poiché il cluster sarà orientato esclusivamente all’elaborazione del dato, all’interno dei Data Services saranno presenti i moduli “Cloudera Data Engineering” (CDE) utilizzabile per le elaborazioni Spark, e “Cloudera Data Warehouse” (CDW) per l’esecuzione di query tramite gli engine Hive e Impala.

* 1. Cloudera Flow Management

Cloudera Flow Management (CFM) è una soluzione di ingestion dati ad alte prestazioni basata su Apache NiFi, progettata per gestire flussi di dati complessi tra diverse sorgenti. Il suo punto di forza è l'interfaccia grafica "drag-and-drop" che permette di costruire pipeline senza scrivere codice, facilitando la movimentazione di dati verso il cloud o data lake. Grazie a oltre 300 processori predefiniti, CFM gestisce con facilità sia dati strutturati che non strutturati in tempo reale.

Per ulteriori dettagli, è possibile consultare la documentazione ufficiale Cloudera e le release note del prodotto ai seguenti link:

**Cloudera CDP Private Cloud Base**:

<https://docs.cloudera.com/cdp-private-cloud-base/7.1.9/runtime-release-notes/topics/rt-pvc-whats-new.html>.

**Cloudera Data Services**:

<https://docs.cloudera.com/cdp-private-cloud-data-services/1.5.5/index.html>

**Cloudera Data Engineering:**

<https://docs.cloudera.com/data-engineering/1.5.5/index.html>

**Cloudera Data Warehouse:**

<https://docs.cloudera.com/data-warehouse/1.5.5/index.html>

**Cloudera Flow Management**:

<https://docs.cloudera.com/cfm/2.1.7/index.html>

* 1. Nuovi servizi presenti

La nuova distribuzione Cloudera introduce diverse novità, tra cui:

* **Ranger**: Permette la gestione dell’autenticazione sul cluster e le autorizzazioni di accesso ai dati
* **Knox**: Fornisce un proxy per accedere in modo sicuro ai principali servizi del cluster
* **Atlas:** Fornisce il lineage del dato e ne permette la classificazione
  + 1. Ranger

Le policy di accesso ai servizi dell’ambiente **Cloudera** sono gestite tramite il servizio **Apache Ranger**.

Apache Ranger è un sistema di sicurezza centralizzato progettato per:

* Gestire e monitorare le policy di accesso ai dati in ambienti Big Data.
* Definire e applicare regole granulari di autorizzazione.
* Garantire che utenti e gruppi abbiano accesso solo ai dati e alle risorse per cui sono autorizzati.

Una caratteristica fondamentale di Ranger è la funzionalità di **audit**, che registra tutte le attività di accesso e modifica, offrendo:

* Visibilità completa su chi ha interagito con i dati e in che modo.
* Supporto alla conformità normativa e monitoraggio di anomalie o usi impropri delle risorse.

Di seguito è mostrata una panoramica dell’architettura di Ranger e della sua integrazione con l’accesso alle basi dati:

![Immagine che contiene testo, schermata, diagramma, Carattere  Descrizione generata automaticamente](data:image/jpeg;base64...)

Gli utenti appartenenti al gruppo AD **“cdp-admin-prod”** e **“cdp-admin-coll”** sono configurati in Ranger per avere privilegi di **full-access** rispettivamente ai cluster di **Produzione** e **Collaudo**.

* + 1. Knox

**Apache Knox** è un gateway di sicurezza progettato per semplificare e proteggere l'accesso ai servizi e alle risorse di un ecosistema Big Data. Funziona come un punto di ingresso centralizzato, permettendo agli utenti di interagire con il cluster tramite un'interfaccia unificata e sicura, senza dover accedere direttamente ai singoli componenti.

Grazie al supporto di protocolli standard come **HTTPS** e alle funzionalità di autenticazione e autorizzazione, Knox assicura un controllo rigoroso degli accessi. Inoltre, il gateway include strumenti di **auditing** che tracciano tutte le richieste, migliorando la visibilità e rafforzando la sicurezza delle interazioni con il cluster.

* + 1. Atlas

Apache Atlas è una piattaforma di data governance e metadata management pensata per ambienti Big Data. Consente di catalogare dataset, tabelle, processi e flussi applicativi, centralizzando i metadati tecnici e funzionali.

Per gli sviluppatori rappresenta uno strumento utile per tracciare la data lineage end-to-end, comprendere dipendenze tra sistemi e integrare metadati tramite API REST o hook nativi con componenti dell’ecosistema Hadoop. In questo modo facilita analisi di impatto, troubleshooting e controllo della qualità del dato.

1. Sicurezza

Questo capitolo descrive le principali informazioni relative alla sicurezza del cluster, con un focus sui servizi di autenticazione e autorizzazione degli accessi ai servizi del cluster.

* 1. Autenticazione

La gestione delle identità e dell'autenticazione nel cluster **Cloudera** si basa sui protocolli **LDAP** (Lightweight Directory Access Protocol) e **Kerberos**:

* **LDAP**: Utilizzato per la gestione delle identità e dei gruppi.
* **Kerberos**: Responsabile della gestione dell'autenticazione degli utenti.

L’autenticazione può avvenire in due modalità:

* Tramite Knox, descritto precedentemente, con le credenziali di dominio dell’utente (username e password) ed è il metodo più comune utilizzabile dall’utente finale:
* Tramite Kerberos, disponibile sia per gli utenti, ma soprattutto per la gestione dei carichi di lavoro sul cluster.
  + 1. Autenticazione Kerberos

Questo tipo di autenticazione può essere fatta sia in modalità interattiva, sia procedurale, tramite un file keytab.

Nella modalità interattiva l’utente esegue il comando “kinit” indicando il proprio username e realm, ed in seguito gli sarà chiesto di imputare la password. Ad esempio:

kinit mrossi@SIIAU.LOCAL

Tramite un file keytab invece, non sarà richiesta alcuna password:

kinit –kt mrossi.keytab mrossi@SIIAU.LOCAL

* + - 1. Creazione File Keytab

Un file .keytab è un contenitore binario usato in ambienti basati su Kerberos per memorizzare in modo sicuro le chiavi segrete associate a uno o più principal (utenti o servizi). Esso permette l’autenticazione non interattiva, evitando l’inserimento manuale della password.

Per questo motivo è fondamentale non condividere il proprio file .keytab, e restringerne l’accesso il più possibile.

Per la creazione del file, seguire le istruzioni riportate di seguito:

Da shell, eseguire *ktutil* per lanciare la sua shell interattiva.

ktutil:

Eseguire quindi il seguente comando per creare il file .keytab:

ktutil: addent -password -p *<username>*@SIIAU.LOCAL -k 1 -e aes256-cts-hmac-sha1-96

Inserire la password per *<username>* quando richiesto, quindi salvare il file specificandone il path e il nome:

ktutil: wkt /path/to/user.keytab

uscire dalla shell interattiva:

ktutil: quit

Verificare quindi la correttezza dell’owner del file generato, e limitare la lettura del file solo ad esso:

chmod 400 /path/to/user.keytab

* 1. TLS

La sicurezza delle comunicazioni, sia tra i servizi del cluster che tra i client e i servizi del cluster, è garantita tramite il protocollo **TLS**.

* La generazione e gestione dei certificati SSL/TLS per gli host del cluster avviene tramite la funzione Auto-TLS di Cloudera.
* I certificati sono stati generati utilizzando la Certification Authority (CA) di AU.

Siccome i certificati sono firmati dalla Certification Authority interna, al primo accesso ai servizi tramite browser, sarà chiesto di verificare il certificato prima di proseguire con la navigazione.

![](data:image/png;base64...)

Visualizzando il certificato potremo vedere le seguenti informazioni

![](data:image/png;base64...)

Si potrà dunque procedere, accettando il certificato

![](data:image/png;base64...)

Nella barra degli indirizzi, sarà comunque mostrato un avviso che indicherà che il certificato, seppur valido, è stato accettato in via eccezionale dall’utente.

![](data:image/png;base64...)

La richiesta di accettazione del certificato avverrà nei casi di primo accesso all’interfaccia del server/dominio, oppure in seguito alla rotazione di certificati, necessaria alla loro scadenza.

1. Infrastruttura

Questa sezione fornisce informazioni sull’infrastruttura di ciascuno dei tre cluster presenti, con un focus sulle principali caratteristiche degli host con cui gli utenti si interfacceranno.

* 1. Versione delle Componenti

Lo schema seguente riassume le versioni delle principali componenti di **Cloudera CDP 7.1.9 SP1**. Per un elenco completo delle componenti e delle relative versioni, consultare la documentazione ufficiale Cloudera al seguente link:

[Cloudera CDP 7.1.9 SP1 - Runtime Component Versions](https://docs.cloudera.com/cdp-private-cloud-base/7.1.9/runtime-release-notes/topics/rt-pvc-whats-new-719sp1.html).

|  |  |
| --- | --- |
| **Component** | **Version** |
| Apache Atlas | 2.1.0.7.1.9.1073-8 |
| Apache Hadoop (Includes YARN and HDFS) | 3.1.1 |
| Apache HBase | 2.4.17 |
| Apache Hive | 3.1.3000 |
| Apache Impala | 4.0.0 |
| Apache Kafka | 3.4.1 |
| Apache Knox | 1.3.0 |
| Apache Livy | 0.7.2 |
| Apache Ozone | 1.4.0 |
| Apache Oozie | 5.1.0 |
| Apache Phoenix | 5.1.1 |
| Apache Ranger | 2.4.0 |
| Apache Spark 2.x | 2.4.8 |
| Apache Spark 3.x | 3.3 |
| Apache Zeppelin | 0.8.2 |
| Apache ZooKeeper | 3.8.1 |

1. Ambiente di Collaudo
   1. Cluster e Servizi

Questo capitolo fornisce una panoramica delle principali componenti e servizi installati sulla piattaforma **Cloudera** per il cluster di Collaudo.

* + 1. Cloudera Manager

La console web **Cloudera Manager**, impiegata per la gestione della piattaforma Cloudera CDP, è attiva sull’host [gdmclocutil03.siiau.local](https://gdmclocutil03.siiau.local:7183/cmf/home).

Il link per accedere alla **GUI** è il seguente:

<https://gdmclocutil03.siiau.local:7183/cmf/home>

Gli utenti del gruppo AD " **cdp-admin-coll**" sono configurati con privilegi di **full** **administrator** sul cluster di Collaudo.

È consigliato l’accesso tramite Knox.

* + 1. Host del Cluster

L’immagine seguente, tratta dalla console di amministrazione dell’ambiente, fornisce un riepilogo degli host che compongono il cluster **Cloudera**:

![](data:image/png;base64...)

* + 1. Nodi Master

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **Ruolo** | **Hostname** | **IP** | **CPU (core)** | **Memory (GiB)** |
| Master | gdmclocmast[01-03].siiau.local | 10.0.1.[101-103] | 16 | 62.1 |

* + 1. Nodi NiFi

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **Ruolo** | **Hostname** | **IP** | **CPU (core)** | **Memory (GiB)** |
| NiFi | gdmclocnifi[01-03].siiau.local | 10.0.1.[118-120] | 4 | 15.2 |

* + 1. Nodi Utility

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **Ruolo** | **Hostname** | **IP** | **CPU (core)** | **Memory (GiB)** |
| Utility | gdmclocutil[01-03].siiau.local | 10.0.1.[113-115] | 16 | 62.1 |

* + 1. Nodi Worker

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **Ruolo** | **Hostname** | **IP** | **CPU (core)** | **Memory (GB)** |
| Worker | gdmclocwork[01-04].siiau.local | 10.0.1.[106-109] | 20 | 125.2 |

* 1. Servizi Installati

Le immagini seguenti mostrano i servizi installati nel cluster e la loro distribuzione sui vari nodi, evidenziando il ruolo di ciascun componente nell'architettura della piattaforma Cloudera.

**Elenco dei servizi installati:**

![](data:image/png;base64...)

![](data:image/png;base64...)

Hbase, Kafka e Ozone non dimensionati per carichi di produzione.

* 1. HDFS

Lo spazio complessivamente allocato su HDFS ammonta a **191.7 TiB**.

* 1. Framework Presenti

Di seguito è riportato l'elenco dei principali framework presenti nel cluster, con le rispettive versioni e percorsi di installazione:

|  |  |  |
| --- | --- | --- |
| **Framework** | **Versione** | **Path** |
| Java | 17.0.13 | - |
| Python | 3.9.18 | /usr/bin/python |

##

* 1. Endpoint Principali
     1. Servizi

Di seguito è riportato l'elenco dei principali servizi del cluster, con gli host associati e le relative porte di comunicazione:

|  |  |  |
| --- | --- | --- |
| Servizio | Host | Porta |
| Hive JDBC/ODBC (Knox) | jdbc:hive2://cloudera-coll.siiau.local:9443/;ssl=1;sslTrustStore=<path\_a\_file\_truststore.jks>;transportMode=http;httpPath=gateway/cdp-proxy-api/hive  Impostare la seguente property del driver:  SSLTrustStorePwd=<password\_truststore>  Autenticarsi con le proprie credenziali AD | - |
| Hive JDBC/ODBC (Kerberos + LB) | jdbc:hive2://cloudera-coll.siiau.local:10003/;principal=hive/\_HOST@SIIAU.LOCAL;ssl=true;sslTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global\_truststore.jks | - |
| Impala JDBC/ODBC (Knox) | jdbc:impala://cloudera-coll.siiau.local:9443/;ssl=1;transportMode=http;httpPath=gateway/cdp-proxy-api/impala;AuthMech=3  Impostare le seguenti properties del driver:  SSLTrustStore=<path\_a\_file\_truststore.jks>  SSLTrustStorePwd=<password\_truststore>  Autenticarsi con le proprie credenziali AD | - |
| Impala JDBC/ODBC (Kerberos + LB) | jdbc:impala://cloudera-coll.siiau.local:21050/default;AuthMech=1;KrbRealm=SIIAU.LOCAL;KrbHostFQDN=\_HOST;KrbServiceName=impala;SSL=1;SSLTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global\_truststore.jks | - |
| Impala shell | impala-shell -i cloudera-coll.siiau.local:21000 -d default -k --ssl --ca\_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global\_cacerts.pem (previo kinit utente) | - |
| HDFS Namenode | Accesso tramite Knox | - |
| WebHDFS (LB + https) | https://cloudera-coll.siiau.local:9443/gateway/cdp-proxy-api/webhdfs/v1/user?op=LISTSTATUS da cli: curl -i -k -L -u <username> "https://cloudera-coll.siiau.local:9443/gateway/cdp-proxy-api/webhdfs/v1/user?op=LISTSTATUS" | - |
| HttpFS (Kerberos + LB) | curl -L -i -k --negotiate -u : "https://cloudera-coll.siiau.local:14003/webhdfs/v1?op=LISTSTATUS" | - |
| Zookeeper | gdmclocmast01.siiau.local  gdmclocmast02.siiau.local  gdmclocmast03.siiau.local | 2182 |
| Kafka Broker | gdmclocutil01.siiau.local  gdmclocutil02.siiau.local  gdmclocutil03.siiau.local | 9093 |

Il file .jks del truststore necessario per le connessioni JDBC è scaricabile dalla home page di Knox:

![](data:image/png;base64...)

La password del truststore è **changeit**.

I connettori JDBC per Hive e Impala sono scaricabili dal seguente link, previa autenticazione con account su cloudera.com:

* + [**Hive JDBC Connector 2.6.30**](https://www.cloudera.com/downloads/connectors/hive/jdbc/2-6-30.html)
  + [**Impala JDBC Connector 2.6.39**](https://www.cloudera.com/downloads/connectors/impala/jdbc/2-6-39.html)

È possibile selezionare l’ultima versione di entrambi, se disponibile.

* + 1. Web UI

L’accesso alle web UI dei servizi presenti sul cluster avviene tramite Knox, in modo da avere un unico punti di autenticazione.

Di seguito l’elenco dei servizi accessibili tramite Knox:

![](data:image/png;base64...)

|  |  |
| --- | --- |
| Servizio | URL |
| Cloudera Manager | <https://gdmclocutil03.siiau.local:7183/cmf/home> |
| Knox | https://cloudera-coll.siiau.local:9443/gateway/homepage/home/?profile=token |

* 1. NFS – HDFS

Su richiesta di AU, per agevolare l’elaborazione e lo scambio dei dati con il cluster Cloudera CDP e i Data Services, su tutti i nodi Worker dell’ambiente di collaudo è stata montata una network share in

/mnt/hdfs

mappata al seguente path HDFS:

/share-nfs

Gli accessi a tale share sono gestiti tramite Ranger.

Per verificare il corretto funzionamento, creare un file di testo in /mnt/hdfs e verificarne la presenza in HDFS mediante il comando

hdfs dfs -ls /share-nfs

1. Ambiente di Produzione
   1. Cluster e Servizi

Questo capitolo fornisce una panoramica delle principali componenti e servizi installati sulla piattaforma **Cloudera** per il cluster di Produzione.

* + 1. Cloudera Manager

La console web **Cloudera Manager**, impiegata per la gestione della piattaforma Cloudera CDP, è attiva sull’host gdmcloputil03.siiau.local.

Il link per accedere alla **GUI** è il seguente:

<https://gdmcloputil03.siiau.local:7183/cmf/home>

Gli utenti del gruppo AD "**cdp-admin-prod**" sono configurati con privilegi di **full** **administrator** sul cluster di Produzione.

È consigliato l’accesso tramite Knox.

* + 1. Host del Cluster

L’immagine seguente, tratta dalla console di amministrazione dell’ambiente, fornisce un riepilogo degli host che compongono il cluster **Cloudera**:

![](data:image/png;base64...)

* + 1. Nodi Master

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **Ruolo** | **Hostname** | **IP** | **CPU (core)** | **Memory (GiB)** |
| Master | gdmclopmast[01-03].siiau.local | 10.0.1.[11-13] | 16 | 62.1 |

* + 1. Nodi Utility

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **Ruolo** | **Hostname** | **IP** | **CPU (core)** | **Memory (GiB)** |
| Utility | gdmcloputil[01-03].siiau.local | 10.0.1.[31-33] | 16 | 62.1 |

* + 1. Nodi NiFi

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **Ruolo** | **Hostname** | **IP** | **CPU (core)** | **Memory (GiB)** |
| NiFi | gdmclopnifi[01-03].siiau.local | 10.0.1.[36-38] | 8 | 30.9 |

* + 1. Nodi Worker

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **Ruolo** | **Hostname** | **IP** | **CPU (core)** | **Memory (GB)** |
| Worker | gdmclopwork[01-09].siiau.local | 10.0.1.[16-24] | 20 | 125.2 |

* 1. Servizi installati

Le immagini seguenti mostrano i servizi installati nel cluster e la loro distribuzione sui vari nodi, evidenziando il ruolo di ciascun componente nell'architettura della piattaforma Cloudera.

**Elenco dei servizi installati:**

![](data:image/png;base64...)

![](data:image/png;base64...)

Hbase, Kafka e Ozone non dimensionati per carichi di produzione.

* 1. HDFS

Lo spazio complessivamente allocato su HDFS ammonta a **862.7 TiB**.

* 1. Framework Presenti

Di seguito è riportato l'elenco dei principali framework presenti nel cluster, con le rispettive versioni e percorsi di installazione:

|  |  |  |
| --- | --- | --- |
| **Framework** | **Versione** | **Path** |
| Java | 17.0.13 | - |
| Python | 3.9.18 | /usr/bin/python |

* 1. Endpoint Principali
     1. Servizi

Di seguito è riportato l'elenco dei principali servizi del cluster, con gli host associati e le relative porte di comunicazione:

|  |  |  |  |
| --- | --- | --- | --- |
| Servizio | Host | Porta | |
| Hive JDBC/ODBC (Knox) | jdbc:hive2://cloudera.siiau.local:9443/;ssl=1;sslTrustStore=<path\_a\_file\_truststore.jks>;transportMode=http;httpPath=gateway/cdp-proxy-api/hive  Impostare la seguente property del driver:  SSLTrustStorePwd=<password\_truststore>  Autenticarsi con le proprie credenziali AD | - | |
| Hive JDBC/ODBC (Kerberos + LB) | jdbc:hive2://cloudera.siiau.local:10003/;principal=hive/\_HOST@SIIAU.LOCAL;ssl=true;sslTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global\_truststore.jks | - | |
| Impala JDBC/ODBC (Knox) | jdbc:impala://cloudera.siiau.local:9443/;ssl=1;transportMode=http;httpPath=gateway/cdp-proxy-api/impala;AuthMech=3  Impostare le seguenti properties del driver:  SSLTrustStore=<path\_a\_file\_truststore.jks>  SSLTrustStorePwd=<password\_truststore>  Autenticarsi con le proprie credenziali AD | - | |
| Impala JDBC/ODBC (Kerberos + LB) | jdbc:impala://cloudera.siiau.local:21050/default;AuthMech=1;KrbRealm=SIIAU.LOCAL;KrbHostFQDN=\_HOST;KrbServiceName=impala;SSL=1;SSLTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global\_truststore.jks | - | |
| Impala shell | impala-shell -i cloudera.siiau.local:21000 -d default -k --ssl --ca\_cert=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global\_cacerts.pem (previo kinit utente) | - | |
| HDFS Namenode | Accesso tramite Knox | - | |
| WebHDFS (LB + https) | https://cloudera.siiau.local:9443/gateway/cdp-proxy-api/webhdfs/v1/user?op=LISTSTATUS da cli: curl -i -k -L -u <username> "https://cloudera.siiau.local:9443/gateway/cdp-proxy-api/webhdfs/v1/user?op=LISTSTATUS" | - |
| Zookeeper | gdmclopmast01.siiau.local  gdmclopmast02.siiau.local  gdmclopmast03.siiau.local | 2182 | |
| Kafka Broker | gdmcloputil01.siiau.local  gdmcloputil02.siiau.local  gdmcloputil03.siiau.local | 9093 | |

Il file .jks del truststore necessario per le connessioni JDBC è scaricabile dalla home page di Knox:

![](data:image/png;base64...)

La password del truststore è **changeit**.

I connettori JDBC per Hive e Impala sono scaricabili dal seguente link, previa autenticazione con account su cloudera.com:

* + [**Hive JDBC Connector 2.6.30**](https://www.cloudera.com/downloads/connectors/hive/jdbc/2-6-30.html)
  + [**Impala JDBC Connector 2.6.39**](https://www.cloudera.com/downloads/connectors/impala/jdbc/2-6-39.html)

È possibile selezionare l’ultima versione di entrambi, se disponibile.

* + 1. Web UI

L’accesso alle web UI dei servizi presenti sul cluster avviene tramite Knox, in modo da avere un unico punti di autenticazione.

Di seguito l’elenco dei servizi accessibili tramite Knox:

![](data:image/png;base64...)

|  |  |
| --- | --- |
| Servizio | URL |
| Cloudera Manager | https://gdmcloputil03.siiau.local:7183/cmf/home |
| Knox | https://cloudera.siiau.local:9443/gateway/homepage/home/?profile=token |

1. Cloudera Data Services
   1. Host del Cluster

I nodi che compongono i Data Services sono presenti all’interno del Cluster di Produzione e sono di seguito riportati:

![](data:image/png;base64...)

Essi verranno comunque utilizzati anche dal cluster di Collaudo, per l’esecuzione dei test previsti per la migrazione.

* 1. Nodi ECS Server

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **Ruolo** | **Hostname** | **IP** | **CPU (core)** | **Memory (GiB)** |
| ECS Server | gdmclopecss[01..03].siiau.local | 10.0.1.[45..47] | 32 | 62.1 |

* 1. Nodi ECS Agent

|  |  |  |  |  |
| --- | --- | --- | --- | --- |
| **Ruolo** | **Hostname** | **IP** | **CPU (core)** | **Memory (GiB)** |
| ECS Agent | gdmclopecsa[01..18].siiau.local | 10.0.1.[51..68] | 40 | 251.1 |

* 1. Runtime Compatibility

Dalla versione 1.5.5 di Cloudera Data Engineering (attualmente installata), le immagini RedHat-based (insecure) per Apache Spark 3.x sono deprecate, e al loro posto sono utilizzate immagini “Security Hardened”.

La differenza tra queste due tipologie di immagini (RedHat-based e Security Hardened) è riportata al seguente link:
<https://docs.cloudera.com/data-engineering/1.5.5/use-resources/topics/cde-security-hardened-image-migration-guide.html>

Ciò impone di prestare attenzione alla compatibilità tra la versione di Cloudera Runtime (7.1.9 SP1) e la versione di Cloudera Data Engineering, come riportato nella seguente immagine:

![](data:image/png;base64...)

Per Spark 2.4.x vengono ancora utilizzate immagini RedHat-based, la cui compatibilità con il Runtime Cloudera è riportata di seguito:

![](data:image/png;base64...)

* 1. Environments

Attualmente in CDS sono stati creati due Environment, uno per Collaudo (**au-collaudo**) e uno per Produzione (**au-produzione**).

Un Environment è un'entità logica che rappresenta l'associazione del proprio account utente Cloudera on-premise con molteplici risorse di calcolo; attraverso queste, è possibile sottoporre a provisioning e gestire carichi di lavoro come Cloudera Data Warehouse, Cloudera Data Engineering e Cloudera AI.

All’interno del servizio di Data Engineering di produzione sono stati creati due cluster, uno per ciascun ambiente (**cde-collaudo** e **cde-produzione**).

In questi ambienti verranno creati i Virtual Cluster necessari per l’esecuzione dei job Spark, in base ai requisiti necessari richiesti da AU.

Al momento, sono stati creati due Virtual Cluster in cde-collaudo (**spark24-test** e **spark35-test**), ognuno dei quali con una specifica versione di Spark (2.4.8 e 3.5.4) per soddisfare le richieste di AU.

Il link di accesso ai CDS di entrambi gli ambienti è il seguente:

<https://console-cdp.apps.cloudera.siiau.local/>

* 1. Framework Presenti

Di seguito è riportato l'elenco dei principali framework presenti nel cluster, suddivisi per ambiente (collaudo e produzione) e installati in appositi Virtual Cluster.

* + 1. Collaudo

|  |  |  |
| --- | --- | --- |
| **Framework** | **Versione** | **Virtual Cluster** |
| Java | Java 1.8.0 | spark24-test |
| Python | 3.6.8 /2.7.18 | spark24-test |
| Java | Java 17 | spark35-test |
| Python | 3.11 | spark35-test |

* + 1. Produzione

|  |  |  |
| --- | --- | --- |
| **Framework** | **Versione** | **Virtual Cluster** |
| Java |  |  |
| Python |  |  |
| Java |  |  |
| Python |  |  |

* 1. Note per l’esecuzione dei job su YARN

L’esecuzione dei job su YARN, come ogni interazione con i servizi standard del cluster (HDFS, Hive, Impala, ecc), necessita dell’autenticazione tramite protocollo Kerberos.

Come indicato al paragrafo [4.1.1](#AutentKerberos), occorre eseguire un comando di kinit tramite credenziali, nel caso di utenza personale o, nel caso di utenza di servizio, tramite file keytab.

Gli script shell che sono usati per eseguire job sul base cluster, quindi dovranno eseguire l’autenticazione Kerberos.

Per evitare conflitti nella gestione della cache delle credenziali in caso di concorrenza dell’esecuzione delle applicazioni, suggeriamo di apportare le seguenti modifiche ai file sh:

set -euo pipefail #istruisce bash di uscire immediatamente in caso di errore, restituendo l’exit code

KEYTAB=path/to/file.keytab

PRINCIPAL=<user>@SIIAU.LOCAL

export KRB5CCNAME=/tmp/krb5cc\_<user>\_$$

#Funzione per la gestione della pulizia della cache

cleanup() {

kdestroy 2>/dev/null || true

rm -f "$KRB5CCNAME"

}

trap cleanup EXIT #Istruzione per invocare la funzione di cleanup anche in caso di errore

kinit -kt "$KEYTAB" "$PRINCIPAL" #Istruzione di kinit

* 1. Note per l’esecuzione dei job su Data Services
     1. Collaudo
        1. Configurazione profilo utente

1. Collegarsi al portale CDE (<https://console-cdp.apps.cloudera.siiau.local/dex/home>)
2. Selezionare **Administration** dal menu di sinistra
3. Per il *Service* a cui si vuole accedere, selezionare il bottone *Service Details* ![](data:image/png;base64...)
4. Nella nuova pagina che si apre, selezionare **Hadoop Authentication**
5. Al primo accesso, verranno visualizzate le seguenti informazioni:

![](data:image/png;base64...)

1. Inserire il proprio *Principal* nel formato *username@SIIAU.LOCAL*
2. In **Authentication Type**, selezionare **Password** e inserire la propria password di dominio
3. Cliccare su **Authenticate**
4. Se i dati inseriti sono corretti, verranno visualizzate le seguenti informazioni:

![](data:image/png;base64...)

**Attenzione**: Alla scadenza della propria password di dominio, sarà necessario eseguire **Revoke Authentication** e reinserire le proprie credenziali aggiornate per ciascun *Service*

1. Dalla stessa pagina, cliccare sul proprio username in basso a sinistra e selezionare **Profile**
2. Dalla pagina caricata, selezionare **Generate Access Key**, quindi nuovamente **Generate Access Key** e cliccare su **Download Credentials File**.

**Attenzione**: una volta chiusa la finestra, la *Private Key* non sarà più recuperabile e occorrerà generare nuove credenziali.

1. Loggarsi via ssh al nodo gdmclocutil03 con le proprie credenziali di dominio
2. Eseguire lo script /mnt/coll\_sw\_config/cde\_user\_setup/config.sh

Lo script creerà nella propria home directory la cartella ***.cde***, al cui interno saranno presenti i seguenti file:

* + AU\_RootCA\_ECS.pem
  + config.yaml
  + credentials

1. Il file credentials andrà aggiornato con le proprie credenziali (**cdp\_access\_key\_id** e **cdp\_private\_key**) generate precedentemente
2. Editare il file **credentials** e sostituirne il contenuto di default con quanto scaricato al punto 11

Eseguire le seguenti spark-submit di test per verificare il corretto funzionamento di quanto configurato, con entrambi i profili spark (spark 2.4 e spark 3.5):

Profilo spark24-test:

CDE\_CONFIG\_PROFILE=spark24-test spark-submit --class org.apache.spark.examples.SparkPi /opt/cloudera/parcels/CDH/jars/spark-examples\*.jar 10

Profilo spark35-test:

CDE\_CONFIG\_PROFILE=spark35-test spark-submit --class org.apache.spark.examples.SparkPi /opt/cloudera/parcels/CDH/jars/spark-examples\*.jar 10

Qualora dovesse presentarsi un errore come quello riportato di seguito, occorre revocare (**Revoke Authentication**) e rigenerare la *Hadoop Authentication* in CDE (Rif. da punto 1):

WARN util.go:134 Failed: Encountered non-retriable error : start TGT gen failed for user username: rpc error: code = Internal desc = java.lang.RuntimeException: javax.security.auth.login.LoginException

Spark submit failed: run job failed: start TGT gen failed for user username: rpc error: code = Internal desc = java.lang.RuntimeException: javax.security.auth.login.LoginException

Si riporta di seguito un esempio di configurazione di job spark da utilizzare per i Data Services:

export CDE\_CREDENTIALS\_FILE=*<path\_to\_credential\_file>*

CDE\_CONFIG\_PROFILE=*<virtual\_environment>* \

spark-submit --executor-cores 2 \

--executor-memory 10g --driver-cores 2 \

--driver-memory 8g \

--conf spark.driver.maxResultSize=1g \

--conf spark.driver.memoryOverhead=1g \

--conf spark.dynamicAllocation.minExecutors=1 \

--conf spark.dynamicAllocation.maxExecutors=10 \

--conf spark.dynamicAllocation.initialExecutors=4 \

--files <file\_1.txt,file\_2.txt,…,file\_n.txt> \

applicazione.py

I valori impostati nella configurazione della spark-submit sono puramente indicativi.

* + 1. Produzione

Da definire

* + 1. Credenziali

Le credenziali da utilizzare per l’esecuzione della spark-submit sono relative ad utenza di servizio. Il Presidio Cloudera fornirà il path al file contenente le credenziali specifico per ogni applicativo.

* + 1. CDE\_CONFIG\_PROFILE

Specifica il nome del Virtual Cluster sul quale verrà eseguito il carico di lavoro.

Come riportato nel paragrafo “Environments”, in ambiente di Collaudo sono stati creati i Virtual Cluster **spark24-test** e **spark35-test**, ognuno dei quali con una specifica versione di Spark (2.4.8 e 3.5.4).

Esempio: CDE\_CONFIG\_PROFILE=spark24-test

* + 1. Files

**Attenzione**: i job eseguiti all’interno dei Data Services non sono in grado di accedere al file system locale dei nodi e al file system NFS.

Sia per i file di configurazione che per i dati, occorre procedere come riportato nei seguenti paragrafi.

* + - 1. Configuration Files

Per passare i file di configurazione è necessario utilizzare la direttiva --files per renderli accessibili al job.

I file potranno essere letti dall’applicazione nel seguente modo:

from pyspark import SparkFiles

path = SparkFiles.get('file\_1.txt')

with open(path, "r") as f:

content = f.read()

* + - 1. Data Files

Per quanto riguarda i file di dati, che potenzialmente hanno dimensioni superiori ai file di configurazione, potranno essere caricati su HDFS, sia tramite script shell dedicati, sia tramite l’eventuale creazione di un flusso NiFi ad hoc.

All’interno delle applicazioni, occorrerà quindi specificare sempre il path HDFS in cui si trovano i dati da leggere.

Ad esempio, il seguente frammento di applicazione PySpark permette di leggere i file .xml presenti nel path HDFS /user/data/input\_files/:

hdfs\_path = "**hdfs://**/user/data/input\_files/\*.xml"

df = spark.read \

    .format("xml") \

    .option("rowTag", "NomeTagPrincipale") \

    .load(hdfs\_path)

La soluzione più adatta allo scopo applicativo verrà concordata in seguito.

* + 1. Configurazioni non supportate

Le configurazioni di Spark Shuffle Service (spark.shuffle.service.enabled) vanno rimosse dal codice ove presenti, poiché non supportate all’interno dei Data Services.

Se tale parametro è impostato a true, il job non verrà eseguito.

* 1. Gestione Job e log Spark via CLI

Per accedere ai log dei job Spark via CLI è necessario utilizzare il client CDE (Cloudera Data Engineering CLI) dal nodo gdmclocutil03.siiau.local (10.0.1.115).

Di seguito gli step da seguire:

1. Connettersi via ssh al nodo gdmclocutil03.siiau.local con le proprie credenziali di dominio
2. Scegliere il profilo di configurazione desiderato tra uno dei seguenti presenti in Collaudo:
   * + - spark24-test
       - spark35-test
3. Eseguire l’export delle credenziali di autenticazione verso il cluster cde:
   export CDE\_CREDENTIALS\_FILE=<path-to-credentials>
4. Dalla CLI possiamo utilizzare cde con differenti opzioni e parametri per gestire i job e i relativi log, come riportato nel paragrafo successivo
   * 1. Comandi CDE CLI

Di seguito alcuni esempi di utilizzo del client cde per gestire i job e per accedere ai log.

**Elencare i job presenti:**

CDE\_CONFIG\_PROFILE=spark35-test cde job list

**Output:**

[

{

"name": "test",

"type": "spark",

"created": "2026-04-29T08:41:38Z",

"modified": "2026-04-29T08:41:38Z",

"retentionPolicy": "keep\_indefinitely",

"mounts": [

{

"resourceName": "test"

}

],

"spark": {

"file": "test.py",

"driverMemory": "1g",

"driverCores": 1,

"executorMemory": "1g",

"executorCores": 1,

"conf": {

"dex.safariEnabled": "false",

"spark.dynamicAllocation.initialExecutors": "1",

"spark.dynamicAllocation.maxExecutors": "200",

"spark.dynamicAllocation.minExecutors": "1",

"spark.pyspark.python": "python3"

},

"logLevel": "INFO"

},

"schedule": {

"enabled": false,

"user": "mrossi"

},

"acls": {

"full\_access": {

"users": [

"\*"

]

},

"view\_only": {}

},

"aclsInfo": {

"accessLevel": "FULL\_ACCESS",

"grantedAt": "2026-06-08T17:33:54.382818816+02:00"

},

"computeConfigurationInfo": {

"appliedConfiguration": "inherit",

"overrideConfiguration": "inherit",

"overrideEnabled": false

}

}

]

**Informazioni su specifico job:**

CDE\_CONFIG\_PROFILE=spark35-test cde job describe --name <job-name>

Esempio:

CDE\_CONFIG\_PROFILE=spark35-test cde job describe --name test

**Output:**

{

"name": "test",

"type": "spark",

"created": "2026-04-29T08:41:38Z",

"modified": "2026-04-29T08:41:38Z",

"retentionPolicy": "keep\_indefinitely",

"mounts": [

{

"resourceName": "test"

}

],

"spark": {

"file": "test.py",

"driverMemory": "1g",

"driverCores": 1,

"executorMemory": "1g",

"executorCores": 1,

"conf": {

"dex.safariEnabled": "false",

"spark.dynamicAllocation.initialExecutors": "1",

"spark.dynamicAllocation.maxExecutors": "200",

"spark.dynamicAllocation.minExecutors": "1",

"spark.pyspark.python": "python3"

},

"logLevel": "INFO"

},

"schedule": {

"enabled": false,

"user": "mrossi"

},

"acls": {

"full\_access": {

"users": [

"\*"

]

},

"view\_only": {}

},

"aclsInfo": {

"accessLevel": "FULL\_ACCESS",

"grantedAt": "2026-06-08T17:43:15.770729324+02:00"

},

"computeConfigurationInfo": {

"appliedConfiguration": "inherit",

"overrideConfiguration": "inherit",

"overrideEnabled": false

}

}

**Avviare un job già creato:**

CDE\_CONFIG\_PROFILE=spark35-test cde job run --name < job-name>

Esempio:

CDE\_CONFIG\_PROFILE=spark35-test cde job run --name test

**Elencare i job eseguiti:**

CDE\_CONFIG\_PROFILE=spark35-test cde run list

**Output (ridotto a un singolo job):**

[

  {

    "id": 3,

    "job": "cli-submit-svc\_cloudera\_coll-1776244006950",

    "type": "spark",

    "status": "failed",

    "user": "svc\_cloudera\_coll",

    "started": "2026-04-15T09:06:51Z",

    "ended": "2026-04-15T09:06:54Z",

    "mounts": [

      {

        "resourceName": "cli-submit-svc\_cloudera\_coll-1776244006950"

      }

    ],

    "spark": {},

    "identity": {

      "disableRoleProxy": true,

      "role": "instance"

    },

    "acls": {

      "full\_access": {}

    },

    "aclsInfo": {

      "accessLevel": "FULL\_ACCESS",

      "grantedAt": "2026-06-04T10:32:07.670766754+02:00"

    },

    "computeConfiguration": "inherit"

  },

...

]

**Dettagli di una run specifica:**

CDE\_CONFIG\_PROFILE=spark35-test cde run describe --id <job-id>

Esempio:

CDE\_CONFIG\_PROFILE=spark35-test cde run describe --id **3**

**Output:**

{

"id": **3**,

"job": "cli-submit-svc\_cloudera\_coll-1776244006950",

"type": "spark",

"status": "failed",

"user": "svc\_cloudera\_coll",

"started": "2026-04-15T09:06:51Z",

"ended": "2026-04-15T09:06:54Z",

"mounts": [

{

"resourceName": "cli-submit-svc\_cloudera\_coll-1776244006950"

}

],

"spark": {},

"identity": {

"disableRoleProxy": true,

"role": "instance"

},

"acls": {

"full\_access": {}

},

"aclsInfo": {

"accessLevel": "FULL\_ACCESS",

"grantedAt": "2026-06-08T18:09:28.374773091+02:00"

},

"computeConfiguration": "inherit"

}

**Elencare tipologie di log per specifico Job run:**

CDE\_CONFIG\_PROFILE=spark35-test cde run logs --show-types --id <job-id>

Esempio:

**Elencare tipologie di log per specifico Job run:**

CDE\_CONFIG\_PROFILE=spark35-test cde run logs --show-types --id 5

**Output:**

TYPE ENTITY STREAM ENTITY DEFAULT

---- ------ ------ --------------

submitter/jobs\_api Submitter Jobs API false

submitter/k8s Submitter Kubernetes false

submitter/stderr Submitter stderr true

submitter/stdout Submitter stdout false

**Visualizzare il log della tipologia desiderata per specifico Job run:**

CDE\_CONFIG\_PROFILE=spark35-test cde run logs --type <type> --id <job-id>

**Esempio:**

CDE\_CONFIG\_PROFILE=spark35-test cde run logs --type submitter/jobs\_api --id 5

**Output**:

2026-04-15 11:44:18 INFO Run created for job name cli-submit-svc\_cloudera\_coll-1776246254422 with run id: 5 and compute configuration: inherit [requestId 77073fb3bab689c11746aab52285bd2f]

2026-04-15 11:44:18 DEBUG In addResourceFilePaths, dirPrefix is , filePaths is map[fin\_consEE\_no\_mm.py:true], filePath is fin\_consEE\_no\_mm.py

[requestId 77073fb3bab689c11746aab52285bd2f]

2026-04-15 11:44:18 DEBUG Ensuring end user service account and role [requestId 77073fb3bab689c11746aab52285bd2f]

2026-04-15 11:44:18 DEBUG Rolebinding cdae5f7ed394a8fe71e13f33f40a64cf475beff9-cdpuser-rb present in cache, using the cached item [requestId 77073fb3bab689c11746aab52285bd2f]

2026-04-15 11:44:18 DEBUG Last used time for job 'cli-submit-svc\_cloudera\_coll-1776246254422' updated [requestId 77073fb3bab689c11746aab52285bd2f]

2026-04-15 11:44:18 DEBUG Last used time for resource 'cli-submit-svc\_cloudera\_coll-1776246254422' updated [requestId 77073fb3bab689c11746aab52285bd2f]

2026-04-15 11:44:18 DEBUG Workspace copy for mount cli-submit-svc\_cloudera\_coll-1776246254422 took 8.424537ms [requestId 77073fb3bab689c11746aab52285bd2f]

2026-04-15 11:44:18 DEBUG Built job run workspace: /app/dex/storage/run/5/workspace [requestId 77073fb3bab689c11746aab52285bd2f]

2026-04-15 11:44:18 DEBUG TGT generation successful, response: &{Principal:svc\_cloudera\_coll@SIIAU.LOCAL StartUUID:41117738-7101-4282-a2e3-7116bb619773 SecretName:tgt-secret-svc.cloudera.coll-15607407920638402186} [requestId 77073fb3bab689c11746aab52285bd2f]

[…]

Di seguito un elenco delle tipologie di log:

* submitter/jobs\_api
* submitter/k8s
* submitter/stderr
* submitter/stdout
* driver/stderr
* driver/stdout
* executor\_*N*/stderr
* executor\_*N*/stdout
  + 1. Visibilità dei log dall’interfaccia CDE

Per garantire ad altri utenti o gruppi (diversi da chi esegue il job) la possibilità di accedere e visualizzare i log all'interno dell'interfaccia CDE, è possibile utilizzare alcuni flag dedicati alla gestione dei permessi di visibilità.

Questi flag permettono di definire due livelli di accesso: **accesso completo** e **sola visualizzazione**,sia per singoli utenti che per interi gruppi:

|  |  |  |  |
| --- | --- | --- | --- |
| Flag | Ambito | Livello di permesso | Note |
| --acl-full-access-group | Gruppo | Accesso completo | Ripetibile per più gruppi |
| --acl-full-access-user | Utente | Accesso completo | Ripetibile per più utenti; \* = tutti gli utenti |
| --acl-view-only-group | Gruppo | Sola visualizzazione | Ripetibile per più gruppi |
| --acl-view-only-user | Utente | Sola visualizzazione | Ripetibile per più utenti; \* = tutti gli utenti |

Se vogliamo dare accesso completo al gruppo data-engineering e all'utente mrossi, e permesso di sola visualizzazione al gruppo data-analysts e all'utente gbianchi, il comando sarà:

CDE\_CONFIG\_PROFILE=spark24-test spark-submit \

--acl-full-access-group data-engineering \

--acl-full-access-user mrossi \

--acl-view-only-group data-analysts \

--acl-view-only-user gbianchi \

-class org.apache.spark.examples.SparkPi \

/opt/cloudera/parcels/CDH/jars/spark-examples\*.jar 10

È possibile ripetere ciascun flag più volte per aggiungere ulteriori utenti o gruppi, ad esempio:

--acl-full-access-group data-engineering --acl-full-access-group platform-team

* + 1. Riferimenti
* [Using the Cloudera Data Engineering command line interface](https://docs.cloudera.com/data-engineering/1.5.5/cli-access/topics/cde-cli.html)
* [Introduction to CDE Job and Resource ACLs](https://community.cloudera.com/t5/Community-Articles/Introduction-to-CDE-Job-and-Resource-ACLs/ta-p/408957)
  1. Installare librerie specifiche in CDE

Nel caso sia necessario l’utilizzo di specifiche librerie Python da eseguire all’interno dei job in CDE, è possibile procedere come descritto nei paragrafi successivi.

* + 1. Requisiti

Per poter scaricare i pacchetti necessari, è fondamentale che i seguenti indirizzi siano raggiungibili dai nodi del cluster:

* pypi.python.org
* pypi.org
* pythonhosted.org
* files.pythonhosted.org

Occorre quindi creare un file che conterrà le risorse che verranno utilizzate dal job, il cui formato è riportato in 8.9.4 (Requirements File Format).

Per semplicità di configurazione, nominare questo file ‘**requirements.txt**’.

Come sempre, è necessario indicare il profilo CDE sul quale si vuole operare e le credenziali di autenticazione verso il cluster.

* + 1. Creazione Virtual Environment Python

I virtual environment sono disponibili esclusivamente per l’esecuzione dei job tramite i comandi cli cde e non tramite la spark-submit indicata al paragrafo 8.7.1.1.

Procedere quindi con la creazione della risorsa con il seguente comando:

CDE\_CONFIG\_PROFILE=<profile-name> cde resource create --name <resource\_name> --type <resource\_type>

Esempio:

CDE\_CONFIG\_PROFILE=spark35-test cde resource create --name *cde-python-env-resource* --type *python-env*

Caricare il file requirements.txt:

CDE\_CONFIG\_PROFILE=spark35-test cde resource upload --name *cde-python-env-resource* --local-path *${HOME}/requirements.txt*

Verificare lo stato di creazione dell’environment:

CDE\_CONFIG\_PROFILE=spark35-test cde resource list-events --name *cde-python-env-resource*

Se appare un messaggio simile al seguente, l’environment è pronto per poter essere utilizzato:

{

"id": 4,

"message": "Job pp-84kgdgf6-resource-builder-cde-python-env-resource-1634911572 succeeded, marking resource with ready status",

"created": "2021-10-22T14:09:13Z"

}

* + 1. Eseguire un spark submit con il Virtual Environment Python creato

Una volta creato il virtual environment Python, possiamo eseguire la spark submit con il seguente comando:

CDE\_CONFIG\_PROFILE=spark35-test cde spark submit *pyspark-example.py* --python-env-resource-name *cde-python-env-resource* --name *pyspark-example*

* + 1. Riferimenti
* [Using Python virtual environments with Cloudera Data Engineering](https://docs.cloudera.com/data-engineering/1.5.5/use-resources/topics/cde-python-virtual-env.html)
* [Requirements File Format](https://pip.pypa.io/en/latest/reference/requirements-file-format/#requirements-file-format)
  1. Immagini Docker Custom

In CDE on-premises è possibile utilizzare delle immagini Docker personalizzate per l’esecuzione di job Spark.

A causa di un problema di incompatibilità di Spark 2.4 con le versioni di Python presenti nelle immagini già installate in CDE, si è resa necessaria la creazione di una immagine custom opportunamente configurata.

Di seguito verranno illustrati i passi per l’esecuzione di spark-submit sulla risorsa creata su immagine custom. Alla risorsa creata è stato assegnato il nome **python2\_exprivia**, ed è utilizzabile da tutti gli utenti del Virtual Cluster.

* + 1. Esecuzione di un job Spark con immagine custom

Il job viene lanciato tramite uno script bash che richiama spark-submit con le estensioni CDE. Il nodo di esecuzione è **gdmclocutil03.siiau.local**.

* + - 1. Script di lancio

#!/bin/bash

NUM\_EXEC=12

NUM\_EXEC\_CORE=5

NUM\_EXEC\_MEM=33g

DRIVER\_CORES=5

DRIVER\_MEMORY=33g

export CDE\_CREDENTIALS\_FILE="/mnt/coll\_sw\_config/credentials/svc\_cloudera\_coll\_exp"

CDE\_CONFIG\_PROFILE=spark24-test \

spark-submit \

--runtime-image-resource-name=python2\_exprivia \

--python-version python2 \

--acl-view-only-user '\*' \

--name "GenerazionePraticheRS" \

--num-executors $NUM\_EXEC \

--executor-cores $NUM\_EXEC\_CORE \

--executor-memory $NUM\_EXEC\_MEM \

--driver-cores $DRIVER\_CORES \

--driver-memory $DRIVER\_MEMORY \

--conf spark.network.timeout=1800001 \

--conf spark.executor.heartbeatInterval=1800000 \

--conf spark.driver.extraJavaOptions=-Droot.logger=FATAL,console \

--files Config/Config.json \

--py-files Pratiche\_RS.zip \

main.py $\*

* + - 1. Parametri CDE specifici

Questi parametri sono estensioni CDE del comando spark-submit standard:

|  |  |  |
| --- | --- | --- |
| **Parametro** | **Valore** | **Descrizione** |
| --runtime-image-resource-name | python2\_exprivia | Nome della risorsa CDE di tipo custom-runtime-image creata |
| --python-version | python2 | Interprete Python da usare nel container |
| --acl-view-only-user | '\*' | Rende il job visibile a tutti gli utenti del Virtual Cluster |
| --name | "GenerazionePraticheRS" | Nome del job come appare nell'interfaccia CDE |

* + - 1. Configurazione delle credenziali

A differenza dei passi precedenti, qui le credenziali vengono esportate come variabile d'ambiente nello script stesso (non in sessione):

export CDE\_CREDENTIALS\_FILE="/mnt/coll\_sw\_config/credentials/svc\_cloudera\_coll\_exp"

Il profilo CDE è impostato inline sulla riga del comando, non come export:

CDE\_CONFIG\_PROFILE=spark24-test \

spark-submit ...

Questo garantisce che il profilo sia attivo solo per quella singola invocazione, senza inquinare l'ambiente della sessione bash.

* + - 1. File distribuiti al job

|  |  |  |
| --- | --- | --- |
| **Parametro** | **File** | **Descrizione** |
| --files | Config/Config.json | File di configurazione distribuito a tutti i worker nel working directory |
| --py-files | Pratiche\_RS.zip | Archivio dei moduli Python del progetto, aggiunto al PYTHONPATH di ogni executor |

Entrambi i path sono relativi alla directory da cui si esegue lo script.

* + - 1. Dimensionamento delle risorse

|  |  |
| --- | --- |
| **Risorsa** | **Valore** |
| Executor | 12 |
| Core per executor | 5 |
| Memoria per executor | 33 GB |
| Core driver | 5 |
| Memoria driver | 33 GB |

I timeout Spark (spark.network.timeout=1800000 ms, spark.executor.heartbeatInterval=1800000 ms) sono impostati a **30 minuti** per tollerare job di lunga durata senza che il driver dichiari gli executor persi.

1. NiFi

NiFi è stato installato alla stessa major version del precedente cluster (1.x) per mantenere la maggior compatibilità possibile relativamente ai processori.

Nonostante questo, devono essere apportate alcune modifiche nei Controller Services e/o nei Processori per adattarli al nuovo ambiente.

* 1. Processori Hive

Poiché il cluster è configurato con autenticazione abilitata e i protocolli di rete utilizzano TLS, è necessario apportare alcune modifiche per consentire sia la corretta identificazione degli utenti sia la comunicazione sicura con i servizi.

Alcuni Controller Services, come ad esempio HiveConnectionPool, oltre a essere deprecati, non supportano TLS. La documentazione ufficiale raccomanda quindi l’utilizzo di ClouderaHiveConnectionPool; questa scelta, tuttavia, comporta l’adozione di processori dedicati, come SelectClouderaHiveQL e PutClouderaHiveQL.

![](data:image/png;base64...)

![](data:image/png;base64...)

È inoltre disponibile il Controller Service Hive3ConnectionPool, ancora compatibile con il processore SelectHiveQL, ma se ne sconsiglia l’utilizzo in quanto non sarà più supportato nelle versioni NiFi 2.x.

* 1. GenerateFlowFile

In alcuni casi, ad esempio quando il processore GenerateFlowFile è il primo del flusso, per evitare che i job vengano eseguiti su tutti i nodi, è necessario modificare le impostazioni di Scheduling del Processore impostando a “Primary Node” il campo “Execution”:

![](data:image/png;base64...)

Questa impostazione non è dovuta dalla nuova versione della piattaforma, ma dal fatto che NiFi è in alta affidabilità su tre nodi.

1. Sqoop
   1. Direct mode

Come riportato nella documentazione Cloudera (rif. CDPD-44431 in <https://docs.cloudera.com/cdp-private-cloud-base/7.1.9/runtime-release-notes/topics/rt-pvc-known-issues-sqoop.html>), l’utilizzo dell’opzione **--direct** in comandi dsqoop di import/export presenta diversi svantaggi:

* Le importazioni possono causare split di input intermittenti e sovrapposti
* Le importazioni possono generare dati duplicati
* Possono verificarsi numerosi problemi, come errori intermittenti
* È richiesta una configurazione aggiuntiva

Il workaround proposto da Cloudera è di non utilizzare il direct mode (che è disattivato di default). Tuttavia, se è necessario utilizzarlo, è possibile abilitarlo nei job mediante la stringa

-Dsqoop.enable.deprecated.direct=true

1. Appendice
   1. Verifiche di connettività

Per verificare la raggiungibilità dei servizi dalla propria postazione di lavoro, in assenza di strumenti quali Telnet, è possibile eseguire il seguente comando tramite Windows Power Shell:

|  |
| --- |
| test-netconnection -ComputerName <HOST> -Port <PORT> |

Dove:

* HOST: FQDN o IP dell’host che si vuole raggiungere
* PORT: porta del servizio

Ad esempio:

|  |
| --- |
| test-netconnection -ComputerName gdmcloputil01.siiau.local -Port 21051 |
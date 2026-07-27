# Aggregatore Consumi Aggiustamento/Bilanciamento Gas

## Introduzione
Questo progetto ha come finalità la pubblicazione dei file dei consumi dei processi di Aggiustamento Gas (AGG) e Bilanciamento Gas (SBG)

## Struttura
Sono presenti quattro moduli, di cui _aggregatore-common_ è il modulo padre. Da esso dipendono i moduli:
* _aggregatore-agg_ - modulo che gestisce la pubblicazione dei file di consumi AGG;
* _aggregatore-sbg_ - idem per SBG
* _query-report-sbg_ - dipende da _aggregatore-sbg_ e si occupa della creazione della reportistica per SBG.

Nel percorso _it.eng.au.aggregatoreConsumiCommon.controller.traits_ di _aggregatore-common_ sono disponibili tutti i principali trait estesi dalle singole classi definite nei quattro moduli. In particolare, [RunnableAggregatorTrait](src/main/scala/it/eng/au/aggregatoreConsumiCommon/controller/traits/RunnableAggregatorTrait.scala) è il trait padre, e contiene le definizioni in comune a tutti i processi, successivamente esteso da:
* [RunnableAggregatorPerfomanceOld](src/main/scala/it/eng/au/aggregatoreConsumiCommon/controller/traits/RunnableAggregatorPerfomanceOld.scala), utilizzato dal processo "dettaglio unico";
* [RunnableAggregatorPerfomance](src/main/scala/it/eng/au/aggregatoreConsumiCommon/controller/traits/RunnableAggregatorPerfomance.scala), creato successivamente, utilizzato da tutti gli altri processi, ovvero "aggregato", "dettaglio G", "dtg", "esclusi" (o "incoerenti exc"), "incoerenti dettaglio" (o "incoerenti gdm") e "incoerenti" (ovvero "incoerenti A+B").

I trait dei singoli processi sono poi estesi dagli oggetti finali, come [IdAggregator](src/main/scala/it/eng/au/aggregatoreConsumiCommon/controller/impl/aggregator/IdAggregator.scala) e [UddAggregator](src/main/scala/it/eng/au/aggregatoreConsumiCommon/controller/impl/aggregator/UddAggregator.scala), che si occupano di eseguire il processo di pubblicazione file per un determinato utente (id, it, rdb, udb, udd).
Ognuno di questi processi ha lo scopo di determinare un certo perimetro di PdR, e pubblicarne i consumi come file csv per i relativi utenti, sterilizzando eventualmente eventuali consumi anomali (nel caso di incoerenti exc e incoerenti gdm). Ad esempio,
* _aggregato_ effettua l'aggregazione di un certo perimetro di PdR (principalmente sulla base dei codici di errore della procedura di calcolo) e calcolo il consumo aggregato mensile;
* _dettaglio g_ pubblica il dettaglio consumi dei PdR con trattamento G;
* _dettaglio unico_ pubblica il dettaglio consumi, assieme all'elenco flussi, per il perimetro di PdR definito dall'_aggregato_;
* _incoerenti exc_ o _esclusi_ individua il perimetro dei PdR esclusi (disgiunto dai PdR individuati nell'_aggregato_) e ne pubblica i consumi sterilizzati;
* _incoerenti gdm_ fa lo stesso per il perimetro dei PdR incoerenti gdm (vedere i documenti tecnici per ulteriori dettagli).
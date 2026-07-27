INT Flow Remediation TODO
=========================

1. Aggiornare gli schema `erpDettaglioINTSchema`, `erpDettaglioINTexceptRagSocDistrSchema` ed `erpAggregatoINTSchema` includendo le colonne `q97`‑`q100` rispettando l’ordine Hive.
2. Adeguare le trasformazioni INT perché popolino e sommino i nuovi quarti e correggere la logica del caso 4.
3. Implementare la pubblicazione “Dettaglio POD Interconnessione” (max executionid → `ERP_DET_POD_INT_PUB`).
4. Valutare il parametro `podExcluded` e decidere se applicarlo o rimuoverlo.

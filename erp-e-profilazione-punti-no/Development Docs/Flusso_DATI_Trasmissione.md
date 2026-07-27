# Flusso DATI — Cosa generare e come trasmettere

## Cosa generare (Aggregato ERP)
- Formato: file `XML` codifica `UTF-8`, validato rispetto allo XSD in `tracciato_xsd_preview_v.1.0.xml`.
- Root XML: `ERP_MISURE_MASTER` con attributi obbligatori `ANNO`, `MESE`, `PIVA_GESTORE_DI_RETE`, `RAGIONE_SOCIALE_GESTORE_DI_RETE` (vedi `tracciato_xsd_preview_v.1.0.xml:3`, `tracciato_xsd_preview_v.1.0.xml:827`, `tracciato_xsd_preview_v.1.0.xml:835`, `tracciato_xsd_preview_v.1.0.xml:843`).
- Record: elementi ripetuti `DETAIL` per ogni giorno (`DATA`) e `ZONA_MERCATO` tra: `NORD, CNOR, CSUD, SUD, SICI, SARD, NORE, CALA` (vedi `tracciato_xsd_preview_v.1.0.xml:9`).
- Misure: attributi `QH1..QH96` obbligatori; `QH97..QH100` opzionali per giorni con 25h (QH100 obbligatorio solo se `MESE='10'`; altrimenti a 0) (vedi `tracciato_xsd_preview_v.1.0.xml:792`, `tracciato_xsd_preview_v.1.0.xml:816`, e nota in `trasmissione misure dati.md:355`). Valori interi 0–99.999.999.

Esempio minimale:
```
<?xml version="1.0" encoding="utf-8"?>
<ERP_MISURE_MASTER ANNO="2025" MESE="10" PIVA_GESTORE_DI_RETE="01234567890" RAGIONE_SOCIALE_GESTORE_DI_RETE="Impresa X">
  <DETAIL DATA="2025-10-01" ZONA_MERCATO="NORD" QH1="0" ... QH96="0" QH97="0" QH98="0" QH99="0" QH100="0"/>
  <!-- Un DETAIL per ogni giorno e zona mercato -->
  ...
  </DETAIL>
</ERP_MISURE_MASTER>
```

## Nomenclatura file (Aggregato ERP)
- Verso Terna: `<GGMMAAAAHHMMSS>_AGGR_ERP_<PIVA_Terna>.xml` (vedi `trasmissione misure dati.md:355`).
- Verso Distributori: `<GGMMAAAAHHMMSS>_AGGR_ERP_<PIVA_Distributore>_<AAAAMM>.xml` (vedi `trasmissione misure dati.md:355`).

## Come trasmettere (messa a disposizione)
- Canale: SII Cloud Storage (“Scambio dati mediante Cloud”), due modalità (vedi `trasmissione misure dati.md:99`):
  - Manuale via portale Web SIICloud (upload/download da browser).
  - Automatica tramite client Cloud con protocollo WebDAV (integrazione applicativa).
- Certificazione: flussi ACCx con pratica mensile e notifiche giornaliere con digest dei file pubblicati (vedi `trasmissione misure dati.md:99`).
  - Principali servizi: AC2 (a Terna), AC4 (a Distributore di Riferimento), AC5 (agli Utenti del Dispacciamento), come da capitolo 6/7 (`trasmissione misure dati.md:5`, `trasmissione misure dati.md:127`).

## Altri formati collegati (per completezza)
- Misure orarie inviate dai Distributori al SII (CO1): file `XML` (eventualmente compresso) con nomenclatura
  `<PIVA_Distributore>_<PIVA_Utente>_<AAAAMM>_<Flusso>_<Timestamp>_<Progressivo>_<CODICE_DP>__<SM>.xml` (vedi `trasmissione misure dati.md:122`).
- IP convenzionali (POD non trattati su base oraria): file `CSV` con nomenclatura
  `<PIVA_Distributore>_IP_<AREA>_<Progressivo>.csv` e tracciato dedicato (vedi `trasmissione misure dati.md:122`).

## Allineamento con il codice del repo
- Le trasformazioni IP aggregano su 100 quarti d’ora quando necessario (`q1..q100`), coerente con i campi `QH1..QH100` dello XSD (vedi `ERP/src/main/scala/it/eng/au/ERP/schema/erp/erpAggregatoIPOSchema.scala:5`).

Se vuoi, posso generare uno scheletro XML conforme (mese/giorni/zone) pronto all’upload sul Cloud SII.


from flask import Flask, flash, request, redirect, url_for, make_response, current_app, session, send_file, jsonify, Response, json, abort
import json
from datetime import timedelta, date, datetime
from functools import update_wrapper
from pydocumentdb import document_client
from pymongo import MongoClient
from datetime import *
from dateutil.relativedelta import *
from impala.dbapi import connect
import pandas as pd

config = {
    "uri" : "mongodb://pdc:AueMongo123@172.16.16.71:27017,172.16.16.72:27017,172.16.16.73:27017/Forniture?replicaSet=rset",
    "database" : "Forniture",
    "CollectionFornitureElettriche" : "FornitureElettriche",
    "CollectionFornitureGas" : "FornitureGas",
    "CollectionAuditElettrico": "AuditElettrico",
    "CollectionAuditGas": "AuditGas",
    "CollectionMisureElettriche": "MisureElettriche",
    "CollectionMisureGas": "MisureGas"
}

# Parametri di connessione con il server 
HOST = "172.16.17.56"
PORT = 21050
USERNAME = "silvia"
PASSWORD = "Cl0udMi$ure"


app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'


QUERY_BASE_GAS = (
    """
    select distinct cod_pdr, annomese_riferimento, data_lettura, dt_caricamento, Flusso, Motivazione, let_tot_prel   
    From
    (
    
    select cod_pdr,  
    mese_comp as    annomese_riferimento, 
    dt_caricamento,
    let_tot_prel,
    data_comp as  data_lettura,
    "" as Motivazione,            
    'Lettura Periodica' as Flusso,
    codice_fornitura
    
    from au_test.letture_tgl 
    
    union all
    
    select cod_pdr,  
    mese_comp as    annomese_riferimento, 
    dt_caricamento,
    let_tot_prel,
    data_comp as  data_lettura,
    (case when motivazione = '1' then 'Misura che sostituisce una stima precedente.'
     when motivazione = '2' then 'Misura che sostituisce una misura fornita precedentemente errata.'
     when motivazione = '3' then 'Misura fornita precedentemente per errore.'
     when motivazione = '4' then 'Ricostruzione per frode.'
     when motivazione = '5' then 'Ricostruzione per malfunzionamento misuratore.'
     else ''
     end) as Motivazione,          
    'Lettura di Rettifica' as Flusso ,
    codice_fornitura
    from au_test.letture_rgl 
    
    union all
    
    select cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_racc as data_lettura,
    "" as Motivazione,               
    'Lettura Periodica' as Flusso ,
    codice_fornitura
    from au_test.letture_tml 
    union all
    
    select    cod_pdr,  
    annomese_riferimento, 
    dt_caricamento,
    let_tot_prel,
    data_racc as  data_lettura,   
    (case when motivazione = '1' then 'Misura che sostituisce una stima precedente.'
     when motivazione = '2' then 'Misura che sostituisce una misura fornita precedentemente errata.'
     when motivazione = '3' then 'Misura fornita precedentemente per errore.'
     when motivazione = '4' then 'Ricostruzione per frode.'
     when motivazione = '5' then 'Ricostruzione per malfunzionamento misuratore.'
     else ''
     end) as Motivazione,
    'Lettura di Rettifica' as FLusso,
    codice_fornitura
    from au_test.letture_rml 
    union all
    
    select cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_lettura,
    "" as Motivazione,     
    'Autolettura' as FLusso,
    codice_fornitura
    from au_test.letture_tal 
    
    union all
    select cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_lettura,               
    "" as Motivazione,     
    'Autolettura' as FLusso,
    codice_fornitura
    from au_test.letture_tav  
    
    union all
    select cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_comp as data_lettura,               
    (case when motivazione = '1' then 'Misura che sostituisce una stima precedente.'
     when motivazione = '2' then 'Misura che sostituisce una misura fornita precedentemente errata.'
     when motivazione = '3' then 'Misura fornita precedentemente per errore.'
     when motivazione = '4' then 'Ricostruzione per frode.'
     when motivazione = '5' then 'Ricostruzione per malfunzionamento misuratore.'
     else ''
     end) as Motivazione,   
    'Lettura di Rettifica Voltura' as FLusso,
    codice_fornitura
    from au_test.letture_rmv  
    
    union all
    select cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_racc as data_lettura,               
    '' as Motivazione,   
    'Lettura Voltura' as FLusso,
    codice_fornitura
    from au_test.letture_vtg 
    )
    as csv
    inner join misure.forniture_gas on csv.codice_fornitura= forniture_gas.codice_fornitura
    and csv.cod_pdr= forniture_gas.t_codice_pdr
    where t_codice_pdr='{}' and t_codice_fiscale='{}'
    order by data_lettura desc
    """
)


QUERY_BASE_EL = (
    """
    select 
        pod ,
        data_lettura,
        data_ricezione,
        motivazione,
        (case when nvl(lettura_f1,0) <> 0 then null
               else lettura_monoraria
         end 
        ) as lettura_monoraria,  
        lettura_f1,  
        lettura_f2,  
        lettura_f3,  
        lettura_f4,  
        lettura_f5,  
        lettura_f6,
        tipo_flusso,
        annomese_riferimento,
        case nvl(EA,'') when '' then repeat(';',95) else EA END EA,
        case nvl(ER,'') when '' then repeat(';',95) else ER END ER
    from misure.misure_storic_f
    where pod='{}' and cf_piva='{}'
    order by data_lettura_num desc;
    """
)
  
    
def connect_impala(host, port, username, password):
    client = connect(
        host=host, port=port, user=username, password=password #, auth_mechanism="PLAIN"
    )
    return client

def open_connection_mongo():
    client = MongoClient(config["uri"], readPreference='secondaryPreferred')
    return client

def df_from_db(query, client):
    df = pd.read_sql(query, client)
    return df


# Lista delle keys da convertire da string a float
convert_fields_elettrico = [
    "delta_misure_f1","delta_misure_f2","delta_misure_f3",
    "delta_misure_f4","delta_misure_f5","delta_misure_f6",
    "delta_misure_monoraria","lettura_misura_f1","lettura_misura_f2",
    "lettura_misura_f3","lettura_misura_f4","lettura_misura_f5",
    "lettura_misura_f6","lettura_misura_monoraria","delta_misure",
    "lettura_mese", "coefficiente_conversione", "tensione", 
    "potenza_disponibile", "potenza_impegnata", 
    "coefficiente_conversione","consumo_giornaliero_gg",
    "potenza_max_erogata"
]

template_misure_mensili_el = {
    "competenza_consumi": "",
    "lettura_misura_monoraria": None,
    "lettura_misura_f1": None,
    "lettura_misura_f2": None,
    "lettura_misura_f3": None,
    "lettura_misura_f4": None,
    "lettura_misura_f5": None,
    "lettura_misura_f6": None,
    "delta_misure_f1": None,
    "delta_misure_f2": None,
    "delta_misure_f3": None,
    "delta_misure_f4": None,
    "delta_misure_f5": None,
    "delta_misure_f6": None,
    "delta_misure_monoraria": None,
    "tipo_misura": None
}

template_misure_giornaliere_el = {
    'competenza_consumi': None,
    'consumo_giornaliero_gg': None,
    'lettura_misura_f1': None,
    'lettura_misura_f2': None,
    'lettura_misura_f3': None,
    'lettura_misura_f4': None,
    'lettura_misura_f5': None,
    'lettura_misura_f6': None,
    'delta_misure_f1': None,
    'delta_misure_f2': None,
    'delta_misure_f3': None,
    'delta_misure_f4': None,
    'delta_misure_f5': None,
    'delta_misure_f6': None,
    'giorno': None,
    'potenza_max_erogata': None,
    'tipo_misura': None
}


template_misure_gas = {
    'competenza_consumi': None,
    'data_lettura': None,
    'delta_misure': None,
    'lettura_mese': None,
    'tipo_misura': None
}


template_misure_gas_gg = {
    'competenza_consumi': None,
    'data_lettura': None,
    'delta_misure': None,
    'lettura_giorno': None,
    'tipo_misura': None
}

# campi da controllare per popolare
# le misure dove non sono presenti i consumi
tocheck = [
    ("delta_misure_f1", "lettura_misura_f1"),
    ("delta_misure_f2", "lettura_misura_f2"),
    ("delta_misure_f3", "lettura_misura_f3"),
    ("delta_misure_f4", "lettura_misura_f4"),
    ("delta_misure_f5", "lettura_misura_f5"),
    ("delta_misure_f6", "lettura_misura_f6"),
    ("delta_misure_monoraria", "lettura_misura_monoraria"),
    ("delta_misure", "lettura_giorno"),
    ("delta_misure", "data_lettura")
]


def convert_misure(misure):
    """
        Converte i valori all'interno della dict 
        da string a float
        Utilizza la lista convert_fields_elettrico
    """
    for mis in misure:
        for tipo_misura in mis.keys():
            for misura in mis[tipo_misura]:
                for tipo_misura_cambiare in convert_fields_elettrico:
                    try:
                        if misura[tipo_misura_cambiare] == "":
                            misura[tipo_misura_cambiare] = None
                        else:
                            misura[tipo_misura_cambiare] = float(misura[tipo_misura_cambiare])
                    except:
                        pass


def remap_fields_el(doc):
    """
    Torna un dizionario identico a quello che 
    riceve, con valori rimappati in base alla 
    mappa qui definita 'short2ext_map' se presenti
    all'interno del doc di input
    """
    misuratore_map = {
        "T": "Tradizionale",         
        "O": "Orario",         
        "E": "Elettronico",          
        "G": "2G"
    }
    mercato_map = {
        "T": "Servizio di maggior tutela",
        "L": "Mercato Libero",
        "S": "Salvaguardia"
    }
    tariffa_map = {
        "TDR": "Domestico Residente",
        "TDNR": "Domestico Non Residente",
        "D1": "Domestica in Bassa Tensione",
        "D3": "Domestica in Bassa Tensione",
        "D2": "Domestica in Bassa Tensione",
        "TDPC": "Domestico con pompa di calore",
        "BTIP": "Illuminazione pubblica in Bassa Tensione",
        "BTA6": "Altri Usi con potenza impegnata > 15kW",
        "BTA5": "Altri Usi con potenza impegnata > 10kW e <= 15kW",
        "BTA4": "Altri Usi con potenza impegnata > 6kW e <= 10kW",
        "BTA3": "Altri Usi con potenza impegnata > 3kW e <= 6kW",
        "BTA1": "Altri Usi con potenza impegnata <= 1,5kW",
        "BTVE": "Ricarica veicoli elettrici",
        "BTA2": "Altri Usi con potenza impegnata > 1,5kW e <= 3",
        "MTIP": "Illuminazione pubblica in Media Tensione",
        "MTA1": "Media tensione con potenza disponibile <=100 kW",
        "MTA3": "Media tensione con potenza disponibile >500 kW",
        "MTA2": "Media tensione con potenza disponibile > 100 e <=500 kW",
        "ALTA": "Alta Tensione",
        "AATE": "Altissima tensione",
        "AAT1": "Altissima tensione con tensione<380kV",
        "AAT2": "Altissima tensione con tensione >=380kV",
        "RIU": "connesso a RIU",
        "ASDC": "connesso a ASDC"
    }
    #trattamento_map = {
    #    "F": "definite dall'autorita",
    #    "O": "trattamento orario",
    #    "M": "non previste",
    #    "C": "non si applicano"
    #}
    trattamento_map = {
        "F": "Regolate da ARERA",
        "M": "Fascia Unica",
        "O": "Da Contratto",
        "C": "non si applicano"
    }
    short2ext_map = {
        "tipo_misuratore": misuratore_map,
        "tipo_mercato": mercato_map,
        "tariffa": tariffa_map,
        "trattamento": trattamento_map
    }
       
    for pod in doc["pod"]:
        forniture = []
        for forn in pod["forniture"]:
            #item = {
            #    key: short2ext_map.get(key, {}).get(value, value)
            #    for key, value in forn.items()
            #}
            item = {}
            for key, value in forn.items():
                if isinstance(value, str):
                    item[key]=short2ext_map.get(key,{}).get(value,value)
                else:
                    item[key]=value

            forniture.append(item)
        pod["forniture"] = forniture
    return doc


def remap_fields_gas(doc):
    """
    Torna un dizionario identico a quello che 
    riceve, con valori rimappati in base alla 
    mappa qui definita 'short2ext_map' se presenti
    all'interno del doc di input
    """
    tipologia_cliente_map = {
        "3": "attivita di servizio pubblico",         
        "2": "usi diversi",         
        "1": "condominio con uso domestico",          
        "0": "domestico"
    }
    mercato_map = {
        "01": "mercato libero o servizio di tutela", 
        "02": "servizio di fornitura ultima istanza", 
        "03": "servizio di default",
        "M1": "mercato libero",         
        "M2": "servizio di fornitura di ultima istanza",
        "M3": "servizio di default",
        "M4": "servizio di tutela"
    }
    tipologia_uso_map = {
        "C1": "Riscaldamento",
        "C2": "Uso cottura cibi e/o produzione di acqua calda sanitaria",
        "C3": "Riscaldamento + uso cottura cibi e/o produzione di acqua calda sanitaria",
        "C4": "Uso condizionamento",
        "C5": "Uso condizionamento + riscaldamento",
        "T1": "Uso tecnologico (artigianale-industriale)",
        "T2": "Uso tecnologico + riscaldamento "
    }
    short2ext_map = {
        "tipo_pdr": tipologia_cliente_map,
        "tipo_fornitura": mercato_map,
        "categoria_uso": tipologia_uso_map
    }    
    for pdr in doc["pdr"]:
        forniture = []
        for forn in pdr["forniture"]:
            item = {
                key: short2ext_map.get(key, {}).get(value, value)
                for key, value in forn.items()
            }
            forniture.append(item)
        pdr["forniture"] = forniture
    return doc


def get_min_date(misure, key_identifier, format_date):
    list_date = []
    for mis in misure:
        data_str = mis.get(key_identifier)
        datetime_object = datetime.strptime(data_str, format_date)
        list_date.append(datetime_object)

    if len(list_date) > 0:
        return min(list_date)
    return None

def get_max_date(misure, key_identifier, format_date):
    list_date = []
    for mis in misure:
        data_str = mis.get(key_identifier)
        datetime_object = datetime.strptime(data_str, format_date)
        list_date.append(datetime_object)

    if len(list_date) > 0:
        return max(list_date)
    return None

def fix_misure_negativi(misure, key):
    """
        Filtra i consumi negativi, i valori negati vengono settati a valore None
    """
   
    for fornitura in misure:
        for mis in fornitura["misure"]:            
            if key in mis:
                # per ogni misura
                for misura in mis[key]:
                    if "delta_misure" in misura:
                        if misura["delta_misure"]: 
                            if misura["delta_misure"] < 0: 
                                misura["delta_misure"] = None
                    if "delta_misure_f1" in misura:
                        if misura["delta_misure_f1"]:
                            if misura["delta_misure_f1"] < 0: 
                                misura["delta_misure_f1"] = None
                    if "delta_misure_f2" in misura:
                        if misura["delta_misure_f2"]:
                            if misura["delta_misure_f2"] < 0: 
                                misura["delta_misure_f2"] = None
                    if "delta_misure_f3" in misura:
                        if misura["delta_misure_f3"]:
                            if misura["delta_misure_f3"] < 0: 
                                misura["delta_misure_f3"] = None
                    if "delta_misure_f4" in misura:
                        if misura["delta_misure_f4"]:
                            if misura["delta_misure_f4"] < 0: 
                                misura["delta_misure_f4"] = None
                    if "delta_misure_f5" in misura:
                        if misura["delta_misure_f5"]:
                            if misura["delta_misure_f5"] < 0: 
                                misura["delta_misure_f5"] = None
                    if "delta_misure_f6" in misura:
                        if misura["delta_misure_f6"]:
                            if misura["delta_misure_f6"] < 0: 
                                misura["delta_misure_f6"] = None

    return misure

def fix_misure_1Gennaio2019(misure):
    """
        Impostare il consumo gg a null per la data piu recente del 1 gennaio 2019

        -> misure_orarie
        ----> giorno
    """
    # Verifica se ci sono le misure orarie (giornaliere)
    key = "misure_orarie"
    format_date = "%Y%m%d"
    datetime_giorno_rif = datetime(2019, 1, 1) # 1 Gen 2019
    misura_giorno_rif = None
    list_mis_fix = []
    for fornitura in misure:
        for mis in fornitura["misure"]:            
            if key in mis:
                # per ogni misura
                for misura in mis[key]:
                    giorno = misura["giorno"]
                    datetime_giorno = datetime.strptime(giorno, format_date)
                    if (datetime_giorno == datetime_giorno_rif):
                        misura_giorno_rif = misura
                    if (datetime_giorno < datetime_giorno_rif):
                        list_mis_fix.append(misura)
        if (len(list_mis_fix) > 0 and misura_giorno_rif is not None):
            try:
                item = max(list_mis_fix, key=lambda x: x["giorno"])
                if (item["lettura_misura_f1"] == 0):
                    misura_giorno_rif["delta_misure_f1"] = None
                if (item["delta_misure_f2"] == 0):
                    misura_giorno_rif["delta_misure_f2"] = None
                if (item["lettura_misura_f3"] == 0):
                    misura_giorno_rif["delta_misure_f3"] = None
                if (item["lettura_misura_f4"] == 0):
                    misura_giorno_rif["delta_misure_f4"] = None
                if (item["lettura_misura_f5"] == 0):
                    misura_giorno_rif["delta_misure_f5"] = None
                if (item["lettura_misura_f6"] == 0):
                    misura_giorno_rif["delta_misure_f6"] = None
            except KeyError:
                pass
    return misure


def add_misure_mancanti(misure, template, key_identifier, key_misure, type_):
    """
    Ritorna un dizionario di misure con la stessa struttura
    del dizionario di misure in input, contenente elementi aggiuntivi
    corrispondenti alle misure mancanti.
    Arguments: 
    - misure: dict; dizionario di tutte le misure
    - template: dizionario contenente la struttura della misura 
                da inserire con tutte le chiavi settate a 'None'.
                Verificare 'template_misure*' definiti all'inizio
                di questo file.
    - key_identifier: stringa; e' la chiave da controllare per
                      verificare la presenza del dato
    - key_misure: stringa; chiave della tipologia di misure
                  da aggiungere. Esempio: "misure_orarie" oppure
                  "misure_mensili"
    - type_: stringa; "month" oppure "day" per popolare le misure 
             mancanti per dati mensili o giornalieri
    """
    datestring_format = "%Y%m" if type_ == "month" else "%Y%m%d"
    time_window_count = 1 
    misure_mancanti = {}

    if not (key_misure in misure): 
        return misure

    # individuare la data maggiore
    first_date = get_min_date(misure.get(key_misure), key_identifier, datestring_format) 
    last_date_indata = get_max_date(misure.get(key_misure), key_identifier, datestring_format)
    today = datetime.utcnow().date()
    today_2months_ago = today - relativedelta(months=1)

    if (key_misure == "misure_mensili"):
        last_date = max(last_date_indata.date(), today_2months_ago)
    else:
        last_date = last_date_indata
    today = last_date
    
    # Se la first_date o last_date sono a NULL non esegue nessuna elaborazione
    if (first_date is None or last_date is  None):
        return misure

    # Calcolare il numero di mesi o giorni tra last_date e first_date
    delta = relativedelta(last_date, first_date)
    time_window = delta.years * 12 + delta.months +1  if type_ == "month" else (last_date - first_date).days
 
    while time_window_count < time_window:
        if type_ == "month":
            previous = today - relativedelta(months=time_window_count)
        elif type_ == "day":
            previous = today - relativedelta(days=time_window_count)
        datestring = previous.strftime(datestring_format)
        misura = dict(template)
        misura[key_identifier] = datestring
        misure_mancanti[datestring] = misura
        time_window_count += 1
    misure_popolate = misure.get(key_misure)
    if misure_popolate is not None:
        if len(misure_popolate) != time_window:
            for mis in misure_popolate:
                key = mis[key_identifier]
                misure_mancanti[key] = mis
            misure[key_misure] = [value for value in misure_mancanti.values()]
        else:
            pass
    else:
        return misure
    return misure


def get_previous_misura(list_misure, item_current, key_identifier, type_):
    #recupera il giorno/mese precedente
    if type_ == "month":
        datetime_object = datetime.strptime(datestring, "%Y%m")
        previous = datetime_object + relativedelta(months=-1)
        datestring = previous.strftime("%Y%m")
    else:
        try:
            datetime_object = datetime.strptime(datestring, "%Y%m%d")
        except ValueError:
            datetime_object = datetime.strptime(datestring[10:], "%Y-%m-%d")
        previous = datetime_object + relativedelta(days=-1)
        datestring = previous.strftime("%Y%m%d") 
    
    item_found = next(item for item in list_misure if item[key_identifier] == datestring)

    
    #se la lettura non e' None e delta non e' None 
    # calcola diff tra item_current - l'elemento recuperato
    return item_found

def ordina_misure(result, key_misure , key_identifier):
    """
    Modifica l'array d'input. 
    Ordina l'array corrispondente al valore della chiave 
    'key_misure' in base al valore della chiave 'key_identifier'.
    """
    for forn in result:
        for misura in forn["misure"]:
            if key_misure in misura:
                misura[key_misure] = sorted(
                    misura[key_misure],
                    key=lambda x: x[key_identifier],
                    reverse=True
            )

def popola_consumi(misure_new, key_misure, key_identifier, keys_tocheck, type_):
    """
    Torna un array di dizionari popolati dalla funzione 'popola_item()'
    Arguments:
    - misure_new: dizionario contenente tutte le misure
    - key_misure: stringa; es.: "misure_orarie"
    - key_identifier: stringa; es.: "competenza_consumi"
    - keys_tocheck: array di tuple, dove il primo elemento e' 
                     la chiave che che contiene il consumo, e il secondo 
                     elemento e' la lettura
    - type_: stringa; "month" o "day" a seconda del check da effettuare


    """
    list_misure = []
    for item in misure_new:
        if (key_misure in item):
            for item_misure_key in item:
                list_misure.append(item_misure_key)

    #app.logger.info(misure_new)
    for delta, lettura in keys_tocheck:
        for item in list_misure:
            #app.logger.info(item)
            #app.logger.info(delta)
    
            if item[delta] is None and item[lettura] is not None:
                # calcola il consumo del giorno/mese precedente 
                previous_misura = get_previous_misura(list_misure, item, key_identifier, type_)
                if previous_misura is not None:
                    item[delta] = float(item[lettura]) - float(previous_misura[lettura])

    return misure_new


def popola_item(new_dict, item, lettura, delta, key_identifier, type_):
    """
    Torna un dizionario dove vengono popolati i valori 
    delle chiavi riguardanti i consumi che sono 'null' usando
    le letture del giorno o mese precedente
    Arguments:
    - new_dict: dizionario che ha come chiave la data della misura, 
                e come valore il dato su quella misura
    - item: dizionario con valori misura
    - lettura: stringa; es.: "lettura_df1" 
    - delta: stringa; es.: "delta_df1"
    - key_identifier: stringa; es.: "competenza_consumi"
    - type_: stringa; es.: "month"
    """
    datestring = item[key_identifier]
    if type_ == "month":
        datetime_object = datetime.strptime(datestring, "%Y%m")
        previous = datetime_object + relativedelta(months=-1)
        datestring = previous.strftime("%Y%m")
    else:
        try:
            datetime_object = datetime.strptime(datestring, "%Y%m%d")
        except ValueError:
            datetime_object = datetime.strptime(datestring[10:], "%Y-%m-%d")
        previous = datetime_object + relativedelta(days=-1)
        datestring = previous.strftime("%Y%m%d")
    lettura_precedente = new_dict[datestring][lettura]
    if lettura_precedente is not None:
        item[delta] = str(float(item[lettura]) - float(lettura_precedente))
    return item


# Decoratore per crossdomain
def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, str):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, str):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator


def update_or_insert(codice_fiscale, collection, new_audit):
    query = { "codice_fiscale": codice_fiscale}
    cf_exists = collection.count(query) > 0

    if cf_exists:
        collection.update({'codice_fiscale': codice_fiscale}, {'$push': {'audit': new_audit}})
    else:
        new_item = {
            'codice_fiscale': codice_fiscale,
            'audit': [
                new_audit
            ]
        }
        collection.insert_one(new_item)
    return True

def AddAuditFornitureElettriche(codice_fiscale, tipo, spid_code, esito, parametri = None):
    global config
    my_date = datetime.now() + timedelta(hours=2) #add 2 hours (Italian zone)
    app.logger.info("Local time: " + my_date.strftime('%Y-%m-%dT%H:%M:%S.%f%z'))

    info = {}
    if parametri is not None:
        info = 'CF: ' + codice_fiscale + ', POD: ' + parametri + ', ESITO: ' + esito
    else:
        info = 'CF: ' + codice_fiscale + ', ESITO: ' + esito
    new_audit = {'tipo' : tipo, 'data' : my_date.strftime('%Y-%m-%dT%H:%M:%S.%f%z'), 'spid_code' : spid_code, 'info' : info }
    #client = MongoClient(config["uri"])
    client = open_connection_mongo()
    db = client[config["database"]]
    # FATTO: Cambiare il nome fornitura
    collection = db[config["CollectionAuditElettrico"]]
    update_or_insert(codice_fiscale, collection, new_audit)


def AddAuditFornitureGas(codice_fiscale, tipo, spid_code, esito, parametri = None):
    global config
    my_date = datetime.now() + timedelta(hours=2) #add 2 hours (Italian zone)
    app.logger.info("Local time: " + my_date.strftime('%Y-%m-%dT%H:%M:%S.%f%z'))

    info = {}
    if parametri is not None:
        info = 'CF: ' + codice_fiscale + ', PDR: ' + parametri + ', ESITO: ' + esito
    else:
        info = 'CF: ' + codice_fiscale + ', ESITO: ' + esito
    new_audit = {'tipo' : tipo, 'data' : my_date.strftime('%Y-%m-%dT%H:%M:%S.%f%z'), 'spid_code' : spid_code, 'info' : info }
    #client = MongoClient(config["uri"])
    client = open_connection_mongo()
    db = client[config["database"]]
    # FATTO: Cambiare il nome fornitura
    collection = db[config["CollectionAuditGas"]]
    update_or_insert(codice_fiscale, collection, new_audit)

@app.route('/api/GetFornitureElettriche')
@crossdomain(origin='*')
def GetFornitureElettriche():
    global config
    codice_fiscale = request.args.get('codice_fiscale')
    spid_code = request.args.get('spid_code')

    doc_ret, cf_exists  = _getFornitureElettriche(codice_fiscale, spid_code)
    try:
        convert_misure(doc_ret["pod"])
    except:
        pass
    if not cf_exists:
        abort(404)

    return jsonify(doc_ret)

def _getFornitureElettriche(codice_fiscale, spid_code):
    #client = MongoClient(config["uri"])
    client = open_connection_mongo()
    db = client[config["database"]]
    collection = db[config["CollectionFornitureElettriche"]]
    query = { "codice_fiscale": codice_fiscale}
    projection = { "pod.forniture.misure" : 0, "pod.processi" : 0, "audit" : 0 }
    doc_ret = {}
    cf_exists = False
    try:
        docs = collection.find(query, projection)
        doc_ret = docs[0]
        doc_ret.pop('_id')

        for item in docs[1:]:
            doc_ret["pod"].extend(item["pod"])

        doc_ret = remap_fields_el(doc_ret)
        
        cf_exists = True
    except IndexError:
        pass
    if not cf_exists:
        AddAuditFornitureElettriche(codice_fiscale, 'GetFornitureElettriche', spid_code, 'Nessuna fornitura trovata')
    else:
        AddAuditFornitureElettriche(codice_fiscale, 'GetFornitureElettriche', spid_code, 'Dati visualizzati')
    return doc_ret, cf_exists


def GetAuditFornitureElettriche(codice_fiscale, spid_code):
    global config
    #client = MongoClient(config["uri"])
    client = open_connection_mongo()
    db = client[config["database"]]
    # FATTO: Cambiare il nome fornitura
    collection = db[config["CollectionAuditElettrico"]]
    query = { "codice_fiscale": codice_fiscale}
    projection = { "audit" : 1 }
    doc_ret = {}
    cf_exists = False
    for doc in collection.find(query, projection):
        doc.pop('_id')
        doc_ret = doc
        cf_exists = True
    if not cf_exists:
        AddAuditFornitureElettriche(codice_fiscale, 'GetAuditFornitureElettriche', spid_code, 'DATI_NON_PRESENTI')
        abort(404)
    if 'audit' in doc_ret:
        return doc_ret['audit']
    return list()

@app.route('/api/GetMisureFornitureElettriche')
@crossdomain(origin='*')
def GetMisureFornitureElettriche():
    global config
    dati_presenti = True
    codice_fiscale = request.args.get('codice_fiscale')
    codice_pod = request.args.get('codice_pod')
    spid_code = request.args.get('spid_code')
    #client = MongoClient(config["uri"])
    client = open_connection_mongo()
    db = client[config["database"]]
    collection = db[config["CollectionMisureElettriche"]]
    result = []
    doc_ret, cf_exists = _getFornitureElettriche(codice_fiscale, spid_code)
    if cf_exists:
        for item_pod in doc_ret['pod']:
            if item_pod["codice_pod"] == codice_pod:
                for item_forniture in item_pod['forniture']:
                    # Creo il dictonary delle forniture
                    forniture_res = {}
                    # Aggiungo la lista delle misure
                    forniture_res['misure'] = []

                    codice_fornitura = item_forniture['codice_fornitura']
                    query = { "codice_fornitura": codice_fornitura }
                    dati_presenti = dati_presenti or collection.count(query) > 0
                    forniture_res["codice_fornitura"] = codice_fornitura
                    if dati_presenti:
                        # per ogni misura della fornitura individuata 
                        for doc in collection.find(query):
                            doc.pop('_id')
                            misure = doc['misure']

                            #misure = add_misure_mancanti(
                            #    misure, template_misure_mensili_el,
                            #    "competenza_consumi", "misure_mensili",
                            #    "month"
                            #)
                            #misure = add_misure_mancanti(
                            #    misure, template_misure_giornaliere_el,
                            #    "giorno", "misure_orarie",
                            #    "day"
                            #)

                            forniture_res['misure'].append(misure)

                        convert_misure(forniture_res['misure'])
                    result.append(forniture_res)

        misure = add_misure_mancanti(
            result, template_misure_mensili_el,
            "competenza_consumi", "misure_mensili",
            "month"
        )
        misure = add_misure_mancanti(
            result, template_misure_giornaliere_el,
            "giorno", "misure_orarie",
            "day"
        )          
        
        # Calcolo consumi 
        popola_consumi(result, "misure_mensili", "competenza_consumi", tocheck, "month")
        popola_consumi(result, "misure_non_orarie", "competenza_consumi", tocheck, "month")
        popola_consumi(result, "misure_orarie", "giorno", tocheck, "day")
        fix_misure_1Gennaio2019(result)

        fix_misure_negativi(result, "misure_mensili")
        fix_misure_negativi(result, "misure_non_orarie")
        fix_misure_negativi(result, "misure_orarie")

        # Ordinare
        ordina_misure(result, "misure_mensili", "competenza_consumi")
        ordina_misure(result, "misure_non_orarie", "competenza_consumi")
        ordina_misure(result, "misure_orarie", "giorno")


    if not dati_presenti or not cf_exists:
        AddAuditFornitureElettriche(codice_fiscale, 'GetMisureFornitureElettriche', spid_code, 'Nessuna fornitura trovata', str(codice_pod))
        abort(404)
    else:
        AddAuditFornitureElettriche(codice_fiscale, 'GetMisureFornitureElettriche', spid_code, 'Dati visualizzati', str(codice_pod))
    return jsonify(result)


@app.route('/api/GetProcessiFornitureElettriche')
@crossdomain(origin='*')
def GetProcessiFornitureElettriche():
    global config
    codice_fiscale = request.args.get('codice_fiscale')
    codice_pod = request.args.get('codice_pod')
    spid_code = request.args.get('spid_code')
    #client = MongoClient(config["uri"])
    client = open_connection_mongo()
    db = client[config["database"]]
    collection = db[config["CollectionFornitureElettriche"]]
    query = { "codice_fiscale": codice_fiscale }
    projection = { "pod.forniture" : 0 }
    ret_val = {}
    dati_presenti = False
    for doc in collection.find(query, projection):
        doc.pop('_id')
        for pod in doc['pod']:
            if pod['codice_pod'] == codice_pod:
                ret_val = pod['processi']
                dati_presenti = True
                # NOTE: Qui inserirei un break
    # FATTO
    if dati_presenti:
        AddAuditFornitureElettriche(codice_fiscale, 'GetProcessiFornitureElettriche', spid_code, 'Dati visualizzati', str(codice_pod))
    else:
        AddAuditFornitureElettriche(codice_fiscale, 'GetProcessiFornitureElettriche', spid_code, 'DATI_NON_PRESENTI', str(codice_pod))
        abort(404)
    return jsonify(ret_val)



@app.route('/api/GetFornitureGas')
@crossdomain(origin='*')
def GetFornitureGas():
    codice_fiscale = request.args.get('codice_fiscale')
    spid_code = request.args.get('spid_code')

    doc_ret, cf_exists = _getFornitureGas(codice_fiscale, spid_code)
    try:
        convert_misure(doc_ret["pdr"])
    except:
        pass

    if not cf_exists:
        abort(404)
    else:
        AddAuditFornitureGas(codice_fiscale, 'GetFornitureGas', spid_code, 'Dati visualizzati')


    return jsonify(doc_ret)

def _getFornitureGas(codice_fiscale, spid_code):
    #client = MongoClient(config["uri"])
    client = open_connection_mongo()
    db = client[config["database"]]
    collection = db[config["CollectionFornitureGas"]]
    query = { "codice_fiscale": codice_fiscale }
    projection = { "pdr.forniture.misure" : 0, "pdr.processi" : 0, "audit" : 0 }
    doc_ret = {}
    cf_exists = False
    try:
        docs = collection.find(query, projection)
        doc_ret = docs[0]
        doc_ret.pop('_id')

        for item in docs[1:]:
            doc_ret["pdr"].extend(item["pdr"])

        doc_ret = remap_fields_gas(doc_ret)
        cf_exists = True
    except IndexError:
        pass

    if not cf_exists:
        # FATTO: Fare audit
        AddAuditFornitureGas(codice_fiscale, 'GetFornitureGas', spid_code, 'Nessuna fornitura trovata')
        abort(404)

    return doc_ret, cf_exists

def GetAuditFornitureGas(codice_fiscale, spid_code):
    global config
    #client = MongoClient(config["uri"])
    client = open_connection_mongo()
    db = client[config["database"]]
    # FATTO: Cambiare il nome fornitura
    collection = db[config["CollectionAuditGas"]]
    query = { "codice_fiscale": codice_fiscale}
    projection = { "audit" : 1 }
    doc_ret = {}
    cf_exists = False
    for doc in collection.find(query, projection):
        doc.pop('_id')
        doc_ret = doc
        cf_exists = True
    if not cf_exists:
        AddAuditFornitureGas(codice_fiscale, 'GetAuditFornitureGas', spid_code, 'DATI_NON_PRESENTI')
        abort(404)
    if 'audit' in doc_ret:
        return doc_ret['audit']
    return list()



@app.route('/api/GetMisureFornitureGas')
@crossdomain(origin='*')
def GetMisureFornitureGas():
    global config
    dati_presenti = True
    codice_fiscale = request.args.get('codice_fiscale')
    spid_code = request.args.get('spid_code')
    codice_pdr = request.args.get('codice_pdr')
    #client = MongoClient(config["uri"])
    client = open_connection_mongo()
    db = client[config["database"]]
    collection = db[config["CollectionMisureGas"]]
    result = []
    doc_ret, cf_exists = _getFornitureGas(codice_fiscale, spid_code)
    if cf_exists:
        for item_pdr in doc_ret['pdr']:
            if item_pdr["codice_pdr"] == codice_pdr:
                for item_forniture in item_pdr['forniture']:
                    # Creo il dictonary delle forniture
                    forniture_res = {}
                    # Aggiungo la lista delle misure
                    forniture_res['misure'] = []

                    codice_fornitura = item_forniture['codice_fornitura']
                    query = { "codice_fornitura": codice_fornitura }
                    dati_presenti = dati_presenti or collection.count(query) > 0
                    forniture_res["codice_fornitura"] = codice_fornitura
                    if dati_presenti:
                        # per ogni misura della fornitura individuata 
                        for doc in collection.find(query):
                            doc.pop('_id')
                            misure = doc['misure']
                            #misure = add_misure_mancanti(
                            #    misure, template_misure_gas,
                            #    "competenza_consumi",
                            #    "misure_altre_frequenze",
                            #    "month"
                            #)
                            #misure = add_misure_mancanti(
                            #    misure, template_misure_gas,
                            #    "competenza_consumi",
                            #    "misure_mensili",
                            #    "month"
                            #)
                            #misure = add_misure_mancanti(
                            #    misure, template_misure_gas_gg,
                            #    "data_lettura",
                            #    "misure_giornaliere",
                            #    "day"
                            #)
                            forniture_res['misure'].append(misure)
                        convert_misure(forniture_res['misure'])
                    result.append(forniture_res)

        misure = add_misure_mancanti(
            misure, template_misure_gas,
            "competenza_consumi",
            "misure_altre_frequenze",
            "month"
        )
        misure = add_misure_mancanti(
            misure, template_misure_gas,
            "competenza_consumi",
            "misure_mensili",
            "month"
        )
        misure = add_misure_mancanti(
            misure, template_misure_gas_gg,
            "data_lettura",
            "misure_giornaliere",
            "day"
        )
        
        # Calcolo consumi
        popola_consumi(result, "misure_altre_frequenze", "competenza_consumi", tocheck, "month")
        popola_consumi(result, "misure_mensili", "competenza_consumi", tocheck, "month")
        popola_consumi(result, "misure_giornaliere", "data_lettura", tocheck, "day")

        fix_misure_negativi(result, "misure_altre_frequenze")
        fix_misure_negativi(result, "misure_mensili")
        fix_misure_negativi(result, "misure_giornaliere")

        # Ordinare
        ordina_misure(result, "misure_altre_frequenze", "competenza_consumi")
        ordina_misure(result, "misure_mensili", "competenza_consumi")
        ordina_misure(result, "misure_giornaliere", "data_lettura")



    if not dati_presenti or not cf_exists:
        AddAuditFornitureGas(codice_fiscale, 'GetMisureFornitureGas', spid_code, 'Nessuna fornitura trovata', str(codice_pdr))
        abort(404)
    else:
        AddAuditFornitureGas(codice_fiscale, 'GetMisureFornitureGas', spid_code, 'Dati visualizzati', str(codice_pdr))
    return jsonify(result)


@app.route('/api/GetProcessiFornitureGas')
@crossdomain(origin='*')
def GetProcessiFornitureGas():
    global config
    codice_fiscale = request.args.get('codice_fiscale')
    codice_pdr = request.args.get('codice_pdr')
    spid_code = request.args.get('spid_code')
    #client = MongoClient(config["uri"])
    client = open_connection_mongo()
    db = client[config["database"]]
    collection = db[config["CollectionFornitureGas"]]
    query = { "codice_fiscale": codice_fiscale}
    projection = { "pdr.forniture" : 0}
    ret_val = {}
    dati_presenti = False
    for doc in collection.find(query, projection):
        doc.pop('_id')
        for pdr in doc['pdr']:
            if pdr['codice_pdr'] == codice_pdr:
                ret_val = pdr['processi']
                dati_presenti = True
                # NOTE: Qui inserirei un break
    # FATTO
    if dati_presenti:
        AddAuditFornitureGas(codice_fiscale, 'GetProcessiFornitureGas', spid_code, 'Dati visualizzati', str(codice_pdr))
    else:
        AddAuditFornitureGas(codice_fiscale, 'GetProcessiFornitureGas', spid_code, 'DATI_NON_PRESENTI', str(codice_pdr))
        abort(404)
    return jsonify(ret_val)


@app.route('/api/GetAudit')
@crossdomain(origin='*')
def GetAudit():
    global config
    codice_fiscale = request.args.get('codice_fiscale')
    spid_code = request.args.get('spid_code')
    audit_forniture_elettriche = GetAuditFornitureElettriche(codice_fiscale, spid_code)
    audit_forniture_gas = GetAuditFornitureGas(codice_fiscale, spid_code)
    audit = audit_forniture_elettriche + audit_forniture_gas
    sorted_audit = sorted(audit, key=lambda k: k['data'])
    return jsonify(sorted_audit)

@app.route('/api/login')
@crossdomain(origin='*')
def login():
    global config
    codice_fiscale = request.args.get('codice_fiscale')
    spid_code = request.args.get('spid_code')
    AddAuditFornitureElettriche(codice_fiscale, 'login', spid_code, "", "")
    return jsonify({})


@app.route('/api/Version')
@crossdomain(origin='*')
def Version():
    return jsonify({'version':'1.0', 'client':'172.16.17.244'})



@app.route("/api/GetStoricoLettureGas")
@crossdomain(origin='*')
def get_csv_gas():
    filename = "query_results.csv"
    app.logger.info("Connecting to: " + HOST + ":" + str(PORT))
    client = connect_impala(HOST, PORT, USERNAME, PASSWORD)
    app.logger.info("Connected to: " + HOST + ":" + str(PORT))
    args = request.args
    codice_fiscale = args.get("codice_fiscale")
    codice_pdr = args.get("codice_pdr")
    query = QUERY_BASE_GAS.format(codice_pdr, codice_fiscale)
    app.logger.info("Esecuzione query")
    df = df_from_db(query, client)
    if not df.empty:
        app.logger.info(
            "Oggetto DataFrame pandas costruito dalla query"
        )
    else:
        app.logger.info("Nessun dato trovato per la query")
        return Response(
            "Nessun dato trovato per la query specificata"
        )
    df.reset_index(inplace=True)
    df.drop(columns=["index"], inplace=True)
    df["motivazione"] = df["motivazione"].apply(lambda x: x.replace(";", ","))
    columns_rename_map = {
        "cod_pdr": "PDR",
        "annomese_riferimento": "ANNOMESE_RIFERIMENTO",
        "data_lettura": "DATA LETTURA",
        "dt_caricamento": "DATA RICEZIONE",
        "flusso": "FLUSSO",
        "motivazione": "MOTIVAZIONE",
        "let_tot_prel": "LETTURA"
    }
    df = df.rename(index=str, columns=columns_rename_map)    
    response = make_response(df.to_csv(index=False, sep=";"))
    response.headers["Content-Disposition"] = "attachment; " +\
                                              "filename=" + filename
    response.headers["Content-Type"] = "text/csv"
    return response


@app.route("/api/GetStoricoLettureElettriche")
@crossdomain(origin='*')
def get_csv_el():
    filename = "query_results.csv"
    app.logger.info("Connecting to: " + HOST + ":" + str(PORT))
    client = connect_impala(HOST, PORT, USERNAME, PASSWORD)
    app.logger.info("Connected to: " + HOST + ":" + str(PORT))
    args = request.args
    codice_fiscale = args.get("codice_fiscale")
    codice_pod = args.get("codice_pod")
    query = QUERY_BASE_EL.format(codice_pod, codice_fiscale)
    app.logger.info("Esecuzione query")
    df = df_from_db(query, client)
    if not df.empty:
        app.logger.info(
            "Oggetto DataFrame pandas costruito dalla query"
        )
    else:
        app.logger.info("Nessun dato trovato per la query")
        return Response(
            "Nessun dato trovato per la query specificata"
        )
    # manipolaizone DF da implementare successivamente
    df.reset_index(inplace=True)
    df.drop(columns=["index"], inplace=True)
    df["motivazione"] = df["motivazione"].apply(lambda x: x.replace(";", ","))
    df["ea"] = df["ea"].apply(lambda x: x.replace(",", ";"))
    df["er"] = df["er"].apply(lambda x: x.replace(",", ";"))
    # columns_rename_map = {
    #     "cod_pdr": "PDR",
    #     "annomese_riferimento": "ANNOMESE_RIFERIMENTO",
    #     "data_lettura": "DATA LETTURA",
    #     "dt_caricamento": "DATA RICEZIONE",
    #     "flusso": "FLUSSO",
    #     "motivazione": "MOTIVAZIONE",
    #     "let_tot_prel": "LETTURA"
    # }
    # df = df.rename(index=str, columns=columns_rename_map)
   
    df = expand_df(df, "ea")
    df = expand_df(df, "er")

    #drop columns EA and ER
    df.drop(columns=["ea", "er"], inplace=True)

    response = make_response(df.to_csv(index=False, sep=";").replace('"',"")) 
    response.headers["Content-Disposition"] = "attachment; " +\
                                              "filename=" + filename
    response.headers["Content-Type"] = "text/csv"
    return response

def expand_df(df, column_name, delimiter=";"):
    elements = df[column_name][0].split(delimiter)
    n_elements = len(elements) + 1
    additional_columns = [
        column_name + "{}".format(i)
        for i in range(1, n_elements)
    ]
    df[additional_columns] = df[column_name].str.split(delimiter, expand=True)
    return df

if __name__ == '__main__':
    app.debug = True
    app.run(host = '0.0.0.0', port=8086, threaded=True)




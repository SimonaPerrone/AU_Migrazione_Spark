import sys
import os
import pandas as pd
# from pyhive import hive
import glob

import logging
import logging.config
import configparser

config = configparser.ConfigParser()
config.read('pprof.ini')

logging.config.fileConfig(fname='logging.conf', disable_existing_loggers=False)
# Get the logger specified in the file
logger = logging.getLogger(__name__)


def create_dataframe_c1(dataframe):
    """
    Genera il dataframe C1
    :param dataframe:
    :return:
    """
    data = dataframe.drop(['T1_1', 'T1_2', 'T1_3'], axis=1)
    data["C1_-1"] = 0
    data["C1_-2"] = 0
    data["C1_-3"] = 0

    data = pd.melt(data, id_vars=['DATA'])
    data["variable"] = data["variable"].str.replace("_", "")
    data["variable"] = data["variable"].str.upper()
    data = data.rename(columns={"variable": "PROF", "value": "C1"})
    data["ClassDiPrelievo"] = data['PROF'].str[-1:]
    data["Data - Copy"] = pd.to_datetime(data['DATA'], format="%d/%m/%Y")
    data["CategoriaUso"] = data['PROF'].str[:2]
    data["Area"] = data['PROF'].str[2:3]
    data["KeyZonaClasse"] = data['PROF'].str[2:4]
    data["KeyDataClasse"] = data["DATA"].map(str) + "_" + data["ClassDiPrelievo"]

    return data


def create_dataframe_c2c4(dataframe):
    """
    Genera il dataframe C2C4
    :param dataframe:
    :return:
    """
    columns = ["DATA", "C2", "C4"]
    data = pd.DataFrame()
    data[columns] = dataframe[columns]

    return data


def create_dataframe_t1(dataframe):
    """
    Genera la tabella T1
    :param dataframe:
    :return:
    """
    columns = ["DATA", "T1_1", "T1_2", "T1_3"]
    data = pd.DataFrame()
    data[columns] = dataframe[columns]
    data = pd.melt(data, id_vars=['DATA'])
    data = data.rename(columns={"variable": "CLASSEDIPRELIEVO", "value": "t1"})
    data["CLASSEDIPRELIEVO"] = data['CLASSEDIPRELIEVO'].str[-1:]
    data["KeyDataClasse"] = data['DATA'].map(str) + "_" + data["CLASSEDIPRELIEVO"]
    return data


def create_dataframe_profilo(dataframe):
    """
    Genera dataframe del profilo
    :param dataframe: dataframe contente tutti del profilo
    :return: dataframe di output
    """

    logging.debug("Create PROFILO dataframe")

    columns = ["PROF", "B1PROF", "B2PROF", "B3PROF", "B4PROF", "Cat_uso"]
    data = pd.DataFrame()
    data[columns] = dataframe[columns]
    data["Zona_clim"] = dataframe["Zona_clim"].replace({pd.np.nan: "-"}).map(str)
    data["Classe_prelievo"] = dataframe["Classe_prelievo"]
    data["KeyZonaClasse"] = data['Zona_clim'].map(str) + data["Classe_prelievo"].map(str)

    return data


def create_dataframe_tfc(dataframe):
    dataframe["Data - Copy"] = pd.to_datetime(dataframe['DATA'], format="%d/%m/%Y")
    return dataframe


def create_c1(dataframe_c1, dataframe_tfc, dataframe_profilo, dataframe_t1):
    """
    Crea il dataframe finale C2C4
    :param dataframe_c1: dataframe C1 di input
    :param dataframe_tfc: dataframe TFC di input
    :param dataframe_profilo: dataframe Profilo di input
    :param dataframe_t1: dataframe T1 di input
    :return: ritorna il dataframe finale
    """
    logging.debug("Create C1 dataframe")

    columns_c1 = ['DATA', 'KeyDataClasse', 'C1', 'KeyZonaClasse']
    columns_tfc = ['DATA', 'ID_REG_CLIM', 'WKR']
    columns_profilo = ['PROF', 'B1PROF', 'B2PROF', 'B3PROF', 'B4PROF', 'Cat_uso', 'Zona_clim', 'Classe_prelievo',
                       'KeyZonaClasse']
    columns_t1 = ['CLASSEDIPRELIEVO', 't1', 'KeyDataClasse']

    merge_c1 = pd.merge(
        dataframe_c1[columns_c1],
        dataframe_tfc[columns_tfc],
        on='DATA',
        how='inner'
    )

    merge_c1 = pd.merge(
        merge_c1,
        dataframe_t1[columns_t1],
        on='KeyDataClasse',
        how='inner',
        suffixes=('_left', '_right')
    )

    merge_c1 = pd.merge(
        merge_c1,
        dataframe_profilo[columns_profilo],
        on='KeyZonaClasse',
        how='inner',
        suffixes=('_left', '_right')
    ).drop_duplicates(subset=['KeyZonaClasse', 'KeyDataClasse'])

    data_c1 = pd.DataFrame()
    data_c1['DATA'] = merge_c1['DATA']
    data_c1['C1'] = merge_c1['C1']
    data_c1['KeyZonaClasse'] = merge_c1['KeyZonaClasse']
    data_c1['KeyDataClasse'] = merge_c1['KeyDataClasse']
    data_c1.drop_duplicates()

    return data_c1


def create_c2c4(dataframe_tfc, dataframe_c2c4):
    """
    Crea il dataframe finale C2C4
    :param dataframe_tfc: dataframe TFC di input
    :param dataframe_c2c4: dataframe C2C4 di input
    :return: ritorna il dataframe finale
    """
    logging.debug("Create C2C4 dataframe")

    columns_c2c4 = ['DATA', 'C2', 'C4']
    columns_tfc = ['DATA', 'ID_REG_CLIM', 'WKR']

    merge_c2c4 = pd.merge(
        dataframe_c2c4[columns_c2c4],
        dataframe_tfc[columns_tfc],
        on='DATA',
        how='inner'
    ).drop_duplicates(subset=['DATA'])

    data_c2c4 = pd.DataFrame()
    data_c2c4[columns_c2c4] = merge_c2c4[columns_c2c4]

    return data_c2c4


def create_t1(dataframe_c1, dataframe_t1):
    """
    Crea il dataframe finale C2C4
    :param dataframe_t1: dataframe T1 di input
    :param dataframe_c1: dataframe C1 di input
    :return: ritorna il dataframe finale
    """
    logging.debug("Create T1 dataframe")

    columns_c1 = ['DATA', 'KeyDataClasse']
    columns_t1 = ['DATA', 'CLASSEDIPRELIEVO', 't1', 'KeyDataClasse']

    merge_t1 = pd.merge(
        dataframe_c1[columns_c1],
        dataframe_t1[columns_t1],
        on='KeyDataClasse',
        how='inner',
        suffixes=('_left', '_right')
    ).drop_duplicates(subset=['KeyDataClasse'])

    data_t1 = pd.DataFrame()
    data_t1['DATA'] = merge_t1['DATA_left']
    data_t1['t1'] = merge_t1['t1']
    data_t1['KeyDataClasse'] = merge_t1['KeyDataClasse']

    return data_t1


def create_tfc(dataframe_c1, dataframe_tfc, dataframe_c2c4):
    """
    Crea il dataframe finale TFC
    :param dataframe_c1: dataframe C1 di input
    :param dataframe_tfc: dataframe TFC di input
    :param dataframe_c2c4: dataframe C2C4 di input
    :return: ritorna il dataframe finale
    """
    logging.debug("Create TFC dataframe")

    columns_c1 = ['DATA', 'KeyZonaClasse']
    columns_tfc = ['DATA', 'ID_REG_CLIM', 'WKR']

    merge_tfc2 = pd.merge(
        dataframe_c1[columns_c1],
        dataframe_tfc[columns_tfc],
        on='DATA',
        how='inner'
    ).drop_duplicates(subset=['DATA', 'ID_REG_CLIM'])

    merge_tfc2 = pd.merge(
        merge_tfc2,
        dataframe_c2c4,
        on='DATA',
        how='inner',
        suffixes=('_left', '_right')
    )

    data_tfc = pd.DataFrame()
    data_tfc['DATA'] = merge_tfc2['DATA']
    data_tfc['ID_REG_CLIM'] = merge_tfc2['ID_REG_CLIM']
    data_tfc['WKR'] = merge_tfc2['WKR']

    return data_tfc


def load_hive(sql):
    return pd.read_sql(sql, hive_connection)


def load_csv(path, separator):
    """
    Carica dal path indicato tutti i file csv e restituisce un dataframe
    :param path: path in cui sono presenti i file csv da caricare
    :return: Dataframe pandas
    """
    all_files = glob.glob(path + "/*.csv")
    li = []
    for filename in all_files:
        data_vpg = pd.read_csv(filename, index_col=None, header=0, sep=separator)
        li.append(data_vpg)
    return pd.concat(li, axis=0, ignore_index=True)


def main(argv):
    logging.info("******* START APP *******")
    version = config['APP']['VERSION']
    debug_directory = config['DEBUG']['DIRECTORY']
    debug_to_csv = config['DEBUG']['ENABLE']

    logging.info("DEBUG ENABLE {}".format(debug_to_csv))
    logging.info("Version {}".format(version))

    if debug_to_csv:
        if not os.path.exists(debug_directory):
            os.makedirs(debug_directory)

    path_tfc = config['TFC']['PATH']
    path_vpg = config['VPG']['PATH']
    path_cprof = config['CPROF']['PATH']
    output_directory = config['OUTPUT']['PATH']
    separator = str(config['APP']['SEPERATOR'])

    logging.info("SEPARATOR COLUMNS IN CSV \"{}\"".format(separator))
    logging.info("PATH INPUT DIRECTORY TFC \"{}\"".format(path_tfc))
    logging.info("PATH INPUT DIRECTORY VPG \"{}\"".format(path_vpg))
    logging.info("PATH INPUT DIRECTORY CPROF \"{}\"".format(path_cprof))
    logging.info("PATH OUTPUT DIRECTORY \"{}\"".format(output_directory))

    df_vpg = load_csv(path_vpg, separator)  # load_hive("SELECT * FROM VPG")
    logging.info("Load VPG")
    if debug_to_csv:
        logging.debug("Load VPG Dataframe count rows: {}".format(len(df_vpg.index)))
        df_vpg.to_csv(path_or_buf=debug_directory + "/vpg_out.csv", sep=separator, index=False)
    df_vpg = df_vpg.rename(columns={"GIORNO_RIFERIMENTO": "DATA"})
    logging.debug("Rename column")

    df_tfc = load_csv(path_tfc, separator)  # load_hive("SELECT * FROM TFC")
    logging.info("Load TFC")
    if debug_to_csv:
        logging.debug("Load TFC Dataframe count rows: {}".format(len(df_tfc.index)))
        df_tfc.to_csv(path_or_buf=debug_directory + "/tfc_out.csv", sep=';', index=False)

    df_prof = load_csv(path_cprof, separator)  # load_hive("SELECT * FROM CPROF")
    logging.info("Load CPROF")
    if debug_to_csv:
        logging.debug("Load CPROF Dataframe count rows: {}".format(len(df_prof.index)))
        df_prof.to_csv(path_or_buf=debug_directory + "/prof_out.csv", sep=';', index=False)

    # df_tfc = create_dataframe_tfc(df_tfc)
    dataframe_c1 = create_dataframe_c1(df_vpg)
    dataframe_c2c4 = create_dataframe_c2c4(df_vpg)
    dataframe_t1 = create_dataframe_t1(df_vpg)
    dataframe_profilo = create_dataframe_profilo(df_prof)

    data_c1 = create_c1(dataframe_c1, df_tfc, dataframe_profilo, dataframe_t1)
    logging.debug("Create data C1 - count rows: {}".format(len(data_c1.index)))
    data_c1.to_csv(path_or_buf=output_directory + "/c1.csv", sep=';', index=False)
    logging.info("Generate c1 csv")

    data_c2c4 = create_c2c4(df_tfc, dataframe_c2c4)
    logging.debug("Create data C2C4 - count rows: {}".format(len(data_c2c4.index)))
    data_c2c4.to_csv(path_or_buf=output_directory + "/c2c4.csv", sep=';', index=False)
    logging.info("Generate c2c4 csv")

    data_t1 = create_t1(dataframe_c1, dataframe_t1)
    logging.debug("Create data t1 - count rows: {}".format(len(data_t1.index)))
    data_t1.to_csv(path_or_buf=output_directory + "/t1.csv", sep=';', index=False)
    logging.info("Generate t1 csv")

    data_tfc = create_tfc(dataframe_c1, df_tfc, dataframe_c2c4)
    logging.debug("Create data tfc - count rows: {}".format(len(data_tfc.index)))
    data_tfc.to_csv(path_or_buf=output_directory + "/tfc.csv", sep=';', index=False)
    logging.info("Generate tfc csv")

    logging.info("Generate tfc csv")
    logging.info("******* END APP *******")


if __name__ == "__main__":
    main(sys.argv[1:])

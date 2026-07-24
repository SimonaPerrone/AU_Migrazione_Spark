from flussi.FlussoA01_0150 import FlussoA01_0150
from flussi.FlussoA40_0150 import FlussoA40_0150
from flussi.FlussoD01_0150 import FlussoD01_0150
from flussi.FlussoSM1_0150 import FlussoSM1_0150


def select_flusso(cod_servizio, cod_flusso):
    flusso_instance = None
    if cod_servizio == "A01":
        #if cod_flusso == "0150":
        flusso_instance = FlussoA01_0150()
    elif cod_servizio == "A40":
        flusso_instance = FlussoA40_0150()
    elif cod_servizio == "D01":
        flusso_instance = FlussoD01_0150()
    elif cod_servizio == "SM1":
        flusso_instance = FlussoSM1_0150()

    return flusso_instance


def select_flusso(filename):
    flusso_instance = None

    if "A01" in filename:
        #if "0150" in filename:
        flusso_instance = FlussoA01_0150()

    elif "A40" in filename:
        #if "0150" in filename:
        flusso_instance = FlussoA40_0150()

    elif "D01" in filename:
        #if "0150" in filename:
        flusso_instance = FlussoD01_0150()

    elif "SM1" in filename:
        #if "0150" in filename:
        flusso_instance = FlussoSM1_0150()

    return flusso_instance


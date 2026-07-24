Per elaborare il flusso TML, ecc..

Utilizzare script ***sched_flussi_gen_data.sh***
``` bash

FLUSSO="TML"
ANNO="2019"
MESE="07"
GIORNO="09"

./sched_flussi_gen_data.sh ${FLUSSO} ${ANNO} ${MESE} ${GIORNO}

```

Per elaborare il flusso TML, ecc per mese

Utilizzare script ***sched_flussi_gen_mese.sh***

``` bash

FLUSSO="TML"
ANNO="2019"
MESE="07"
GIORNO=","

./sched_flussi_gen_mese.sh ${FLUSSO} ${ANNO} ${MESE} ${GIORNO}


```

Per elaboare il flusso IM1 per giorno
``` bash
./sched_flussi_gen_im.sh $ANNO $MESE $GIORNO
```

Per elaborare il flusso IM1 mese
``` bash
./sched_flussi_gen_im_mese.sh $ANNO $MESE ,
```

Start TDS
``` bash
./sched_flussitds_gas.sh 2020 01
```

Start TFC
``` bash
./sched_flussitfc_gas.sh 2020 01
```

Start VPG
``` bash
./sched_flussivpg_gas.sh 2020 01
```

Start SAG
``` bash
./sched_flussisag_gas.sh 2020 01
```

## Start flussi documentali
I flussi sono A01, A40, D01, SM1
### Per GIORNO
``` bash
./sched_flussi_gen_2.sh ${FLUSSO} ${ANNO} ${MESE} ${GIORNO}
```

### Per MESE
``` bash
./sched_flussi_gen_2_mese.sh ${FLUSSO} ${ANNO} ${MESE}
```
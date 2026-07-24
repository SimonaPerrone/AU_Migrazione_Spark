OTOCOL="TCP"
HOST="scancl01-01.siiau.local"
SERVICE_NAME="SIIP"
MAPPERS=15
PORT=1521
USERNAME="TMPOD_CLOUD"
PASSWORD="TMPOD_CLOUD01"
TABLE="TISG.PRT_SAG_SETTLEMENT_ANNUALE"
CATALOG="TAB_DATI_SETTLE_SAG_RES_ORACLE_LAST"
SCHEMA="au"
#DATAELABORAZIONE="$1"
#PARTITION="/user/silvia/au/misure_gas_au/TAB_DATI_SETTLE_SAG_RES_ORACLE/$DATAELABORAZIONE"


#Scrittura in Tabella Oracle
sqoop export --connect jdbc:oracle:thin:@\(DESCRIPTION=\(ADDRESS=\(PROTOCOL=${PROTOCOL}\)\(HOST=${HOST}\)\(PORT=${PORT}\)\)\(CONNECT_DATA=\(SERVICE_NAME=${SERVICE_NAME}\)\)\) --username ${USERNAME} --password ${PASSWORD} --table ${TABLE} -hcatalog-table ${CATALOG} -hcatalog-database ${SCHEMA} --num-mappers $MAPPERS 
#sqoop export --connect jdbc:oracle:thin:@\(DESCRIPTION=\(ADDRESS=\(PROTOCOL=TCP\)\(HOST=scancl01-01.siiau.local\)\(PORT=1521\)\)\(CONNECT_DATA=\(SERVICE_NAME=SIIP\)\)\) --username TMPOD_CLOUD --password TMPOD_CLOUD01 --table TISG.PRT_SAG_SETTLEMENT_ANNUALE -hcatalog-table TAB_DATI_SETTLE_SAG_RES_ORACLE -hcatalog-database au  --num-mappers $MAPPERS


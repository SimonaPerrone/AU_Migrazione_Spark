FILE_ELAB=$1 #"sag1_da_rielab_201910101136.csv"
PATH_L="/mnt/TISG_SAG1/"
#mkdir -p `pwd`\SAG1_`date "+%Y%m%d"`
PATH_SAG1=`pwd`"/SAG1_"`date "+%Y%m%d"`

mkdir ${PATH_SAG1}


while read p;
do
   cp ${PATH_L}${p} ${PATH_SAG1}
   
done < ${FILE_ELAB}  

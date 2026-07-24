from REQs.classes.functions import jobTDS
from REQs.classes.functions import jobVPG
from REQs.classes.functions import jobTFC
from REQs.classes.functions import jobTCG
from REQs.classes.functions import jobCGR
from REQs.classes.functions import JobSAG

def switcher(function_index, conf):

	dictonary = {
		"TDS": jobTDS.JobTDS(conf),
		"VPG": jobVPG.JobVPG(conf),
		"TFC": jobTFC.JobTFC(conf),
		"TCG": jobTCG.JobTCG(conf),
		"CGR": jobCGR.JobCGR(conf),
		"SAG": jobCGR.JobSAG(conf)
	}

	return dictonary[function_index]

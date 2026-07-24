import jobTDS
import jobVPG
import jobTFC
import jobTCG
import jobCGR
import jobSAG

def switcher(function_index, conf):

	dictonary = {
		"TDS": jobTDS.JobTDS(conf),
		"VPG": jobVPG.JobVPG(conf),
		"TFC": jobTFC.JobTFC(conf),
		"TCG": jobTCG.JobTCG(conf),
		"CGR": jobCGR.JobCGR(conf),
		"SAG": jobSAG.JobSAG(conf)
	}

	return dictonary[function_index]

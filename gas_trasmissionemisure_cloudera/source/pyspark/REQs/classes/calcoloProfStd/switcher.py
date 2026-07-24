import job13
import job14
import job15
import job16
import job17
import job18
import job19
import job20
import job21
import job22
import job23
import job24

def switcher(function_index, conf):

	dictonary = {
		"job13": job13.Job13(conf),
		"job14": job14.Job14(conf),
		"job15": job15.Job15(conf),
		"job16": job16.Job16(conf),
		"job17": job17.Job17(conf),
		"job18": job18.Job18(conf),
		"job19": job19.Job19(conf),
		"job20": job20.Job20(conf),
		"job21": job21.Job21(conf),
		"job22": job22.Job22(conf),
		"job23": job23.Job23(conf),
		"job24": job24.Job24(conf)
	}

	return dictonary[function_index]

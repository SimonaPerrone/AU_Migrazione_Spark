hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a_201808 AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, regexp_replace(regexp_replace(a.pivadistributorequarti,'01812230223','01932800228'),'01671780193','01341400198') AS pivadistributorequarti , a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2018 and mesequarti = 8 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2018 and a.mesequarti = 8 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201808 &&

	hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a_201809 AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, regexp_replace(regexp_replace(a.pivadistributorequarti,'01812230223','01932800228'),'01671780193','01341400198') AS pivadistributorequarti , a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2018 and mesequarti = 9 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2018 and a.mesequarti = 9 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201809 &&
	
	hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2018 and mesequarti = 10 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2018 and a.mesequarti = 10 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201810 &&
	
	hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2018 and mesequarti = 11 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2018 and a.mesequarti = 11 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201811 &&
	
		hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2018 and mesequarti = 12 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2018 and a.mesequarti = 12 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201812 &&
	
hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 1 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 1 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201901 &&

hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 2 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 2 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201902 &&

hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 3 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 3 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201903 &&

hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 4 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 4 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201904 &&

hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 5 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 5 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201905 &&

hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 6 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 6 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201906 &&

hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 7 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 7 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201907 &&


hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 8 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 8 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201908 &&

	hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 9 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 9 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201909 &&
	
	hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 10 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 10 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201910 &&
	
	hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 11 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 11 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201911 &&
	
		hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk where annoquarti = 2019 and mesequarti = 12 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 and a.annoquarti = 2019 and a.mesequarti = 12 " && ./flusso-misure.sh -S2 -SMS S1 -AM_S 201912 &&

hive -v -e "CREATE or replace VIEW au.flusso_misure_quarti_bck_ok20190306_view_m3_a AS select a.coducquarti, a.podquarti, a.pivautentequarti, a.tipodato_e, a.tipodato_s, a.tensione, a.trattamento_o, a.potcontrimpl, a.potdisp, a.cifreatt, a.cifrerea, a.raccolta, a.validato, a.potmax, a.perdita, a.nomefile, a.annomesegiornodir, a.dataelaborazione, a.time_stamp, a.giornoquarti, a.e1, a.e2, a.e3, a.e4, a.e5, a.e6, a.e7, a.e8, a.e9, a.e10, a.e11, a.e12, a.e13, a.e14, a.e15, a.e16, a.e17, a.e18, a.e19, a.e20, a.e21, a.e22, a.e23, a.e24, a.e25, a.e26, a.e27, a.e28, a.e29, a.e30, a.e31, a.e32, a.e33, a.e34, a.e35, a.e36, a.e37, a.e38, a.e39, a.e40, a.e41, a.e42, a.e43, a.e44, a.e45, a.e46, a.e47, a.e48, a.e49, a.e50, a.e51, a.e52, a.e53, a.e54, a.e55, a.e56, a.e57, a.e58, a.e59, a.e60, a.e61, a.e62, a.e63, a.e64, a.e65, a.e66, a.e67, a.e68, a.e69, a.e70, a.e71, a.e72, a.e73, a.e74, a.e75, a.e76, a.e77, a.e78, a.e79, a.e80, a.e81, a.e82, a.e83, a.e84, a.e85, a.e86, a.e87, a.e88, a.e89, a.e90, a.e91, a.e92, a.e93, a.e94, a.e95, a.e96, a.e97, a.e98, a.e99, a.e100, a.er1, a.er2, a.er3, a.er4, a.er5, a.er6, a.er7, a.er8, a.er9, a.er10, a.er11, a.er12, a.er13, a.er14, a.er15, a.er16, a.er17, a.er18, a.er19, a.er20, a.er21, a.er22, a.er23, a.er24, a.er25, a.er26, a.er27, a.er28, a.er29, a.er30, a.er31, a.er32, a.er33, a.er34, a.er35, a.er36, a.er37, a.er38, a.er39, a.er40, a.er41, a.er42, a.er43, a.er44, a.er45, a.er46, a.er47, a.er48, a.er49, a.er50, a.er51, a.er52, a.er53, a.er54, a.er55, a.er56, a.er57, a.er58, a.er59, a.er60, a.er61, a.er62, a.er63, a.er64, a.er65, a.er66, a.er67, a.er68, a.er69, a.er70, a.er71, a.er72, a.er73, a.er74, a.er75, a.er76, a.er77, a.er78, a.er79, a.er80, a.er81, a.er82, a.er83, a.er84, a.er85, a.er86, a.er87, a.er88, a.er89, a.er90, a.er91, a.er92, a.er93, a.er94, a.er95, a.er96, a.er97, a.er98, a.er99, a.er100, a.annoquarti, a.mesequarti, a.pivadistributorequarti, a.codcontrdispquarti, a.areaquarti
FROM AU.flusso_misure_quarti_bck_ok20190306 A LEFT OUTER JOIN 

(
select 
kk.annoquarti ,kk.mesequarti ,kk.pivadistributorequarti ,kk.codcontrdispquarti ,kk.areaquarti ,kk.podquarti
,kk.nomefile ,kk.dataelaborazione ,kk.progr_podsez  ,kk.motivazione
from 
AU.flusso_misure_estensione_quarti kk 
	) B
	ON a.annoquarti=b.annoquarti AND a.mesequarti=b.mesequarti AND a.pivadistributorequarti=b.pivadistributorequarti
	AND a.codcontrdispquarti=b.codcontrdispquarti AND a.areaquarti=b.areaquarti AND a.podquarti=b.podquarti
	AND a.nomefile =b.nomefile AND a.dataelaborazione =b.dataelaborazione AND a.cifrerea = b.progr_podsez
	where nvl(b.motivazione,'5')!=3 "
hive -v -e "
CREATE or replace VIEW au.flusso_misure_quarti_201701_201807_vs AS SELECT x.coducquarti, x.podquarti, x.pivautentequarti, x.tipodato_e, x.tipodato_s, x.tensione, x.trattamento_o, x.potcontrimpl, x.potdisp, x.cifreatt, x.cifrerea, x.raccolta, x.validato, x.potmax, x.perdita, x.nomefile, x.tipo_pratica, x.motivazione, 
cast(SUBSTR(x.time_stamp, 1,8) as int)
annomesegiornodir
, x.dataelaborazione, x.time_stamp, x.giornoquarti, x.e1, x.e2, x.e3, x.e4, x.e5, x.e6, x.e7, x.e8, x.e9, x.e10, x.e11, x.e12, x.e13, x.e14, x.e15, x.e16, x.e17, x.e18, x.e19, x.e20, x.e21, x.e22, x.e23, x.e24, x.e25, x.e26, x.e27, x.e28, x.e29, x.e30, x.e31, x.e32, x.e33, x.e34, x.e35, x.e36, x.e37, x.e38, x.e39, x.e40, x.e41, x.e42, x.e43, x.e44, x.e45, x.e46, x.e47, x.e48, x.e49, x.e50, x.e51, x.e52, x.e53, x.e54, x.e55, x.e56, x.e57, x.e58, x.e59, x.e60, x.e61, x.e62, x.e63, x.e64, x.e65, x.e66, x.e67, x.e68, x.e69, x.e70, x.e71, x.e72, x.e73, x.e74, x.e75, x.e76, x.e77, x.e78, x.e79, x.e80, x.e81, x.e82, x.e83, x.e84, x.e85, x.e86, x.e87, x.e88, x.e89, x.e90, x.e91, x.e92, x.e93, x.e94, x.e95, x.e96, x.e97, x.e98, x.e99, x.e100, x.er1, x.er2, x.er3, x.er4, x.er5, x.er6, x.er7, x.er8, x.er9, x.er10, x.er11, x.er12, x.er13, x.er14, x.er15, x.er16, x.er17, x.er18, x.er19, x.er20, x.er21, x.er22, x.er23, x.er24, x.er25, x.er26, x.er27, x.er28, x.er29, x.er30, x.er31, x.er32, x.er33, x.er34, x.er35, x.er36, x.er37, x.er38, x.er39, x.er40, x.er41, x.er42, x.er43, x.er44, x.er45, x.er46, x.er47, x.er48, x.er49, x.er50, x.er51, x.er52, x.er53, x.er54, x.er55, x.er56, x.er57, x.er58, x.er59, x.er60, x.er61, x.er62, x.er63, x.er64, x.er65, x.er66, x.er67, x.er68, x.er69, x.er70, x.er71, x.er72, x.er73, x.er74, x.er75, x.er76, x.er77, x.er78, x.er79, x.er80, x.er81, x.er82, x.er83, x.er84, x.er85, x.er86, x.er87, x.er88, x.er89, x.er90, x.er91, x.er92, x.er93, x.er94, x.er95, x.er96, x.er97, x.er98, x.er99, x.er100,
CASE WHEN x.pivadistributorequarti='01671780193' and x.annoquarti = '2018' THEN '01341400198' 
WHEN x.pivadistributorequarti='01341400198' and x.annoquarti = '2017' THEN '01671780193'
ELSE  x.pivadistributorequarti END  pivadistributorequarti,
 x.codcontrdispquarti, x.areaquarti, x.annoquarti, x.mesequarti 
FROM au.rcu_flusso_misure_quarti17_18 x where annoquarti = ${1} and mesequarti = ${2} 

UNION ALL

select z.coducquarti,z.podquarti,z.pivautentequarti,z.tipodato_e,z.tipodato_s,z.tensione,z.trattamento_o,z.potcontrimpl,z.potdisp,z.cifreatt,z.cifrerea,z.raccolta,z.validato,z.potmax,z.perdita,z.nomefile,'R' AS tipo_pratica,'' motivazione,z.annomesegiornodir,z.dataelaborazione,z.time_stamp,z.giornoquarti,z.e1,z.e2,z.e3,z.e4,z.e5,z.e6,z.e7,z.e8,z.e9,z.e10,z.e11,z.e12,z.e13,z.e14,z.e15,z.e16,z.e17,z.e18,z.e19,z.e20,z.e21,z.e22,z.e23,z.e24,z.e25,z.e26,z.e27,z.e28,z.e29,z.e30,z.e31,z.e32,z.e33,z.e34,z.e35,z.e36,z.e37,z.e38,z.e39,z.e40,z.e41,z.e42,z.e43,z.e44,z.e45,z.e46,z.e47,z.e48,z.e49,z.e50,z.e51,z.e52,z.e53,z.e54,z.e55,z.e56,z.e57,z.e58,z.e59,z.e60,z.e61,z.e62,z.e63,z.e64,z.e65,z.e66,z.e67,z.e68,z.e69,z.e70,z.e71,z.e72,z.e73,z.e74,z.e75,z.e76,z.e77,z.e78,z.e79,z.e80,z.e81,z.e82,z.e83,z.e84,z.e85,z.e86,z.e87,z.e88,z.e89,z.e90,z.e91,z.e92,z.e93,z.e94,z.e95,z.e96,z.e97,z.e98,z.e99,z.e100,z.er1,z.er2,z.er3,z.er4,z.er5,z.er6,z.er7,z.er8,z.er9,z.er10,z.er11,z.er12,z.er13,z.er14,z.er15,z.er16,z.er17,z.er18,z.er19,z.er20,z.er21,z.er22,z.er23,z.er24,z.er25,z.er26,z.er27,z.er28,z.er29,z.er30,z.er31,z.er32,z.er33,z.er34,z.er35,z.er36,z.er37,z.er38,z.er39,z.er40,z.er41,z.er42,z.er43,z.er44,z.er45,z.er46,z.er47,z.er48,z.er49,z.er50,z.er51,z.er52,z.er53,z.er54,z.er55,z.er56,z.er57,z.er58,z.er59,z.er60,z.er61,z.er62,z.er63,z.er64,z.er65,z.er66,z.er67,z.er68,z.er69,z.er70,z.er71,z.er72,z.er73,z.er74,z.er75,z.er76,z.er77,z.er78,z.er79,z.er80,z.er81,z.er82,z.er83,z.er84,z.er85,z.er86,z.er87,z.er88,z.er89,z.er90,z.er91,z.er92,z.er93,z.er94,z.er95,z.er96,z.er97,z.er98,z.er99,z.er100,


CASE WHEN z.pivadistributorequarti='01671780193' and z.annoquarti = '2018' THEN '01341400198' 
WHEN z.pivadistributorequarti='01341400198' and z.annoquarti = '2017' THEN '01671780193'
ELSE  z.pivadistributorequarti END  pivadistributorequarti,


z.codcontrdispquarti,z.areaquarti,z.annoquarti,z.mesequarti
from au.flusso_misure_quarti_bck_ok20190306 z
where concat(z.annoquarti,z.mesequarti) in (
'20171',
'20172',
'20173',
'20174',
'20175',
'20176',
'20177',
'20178',
'20179',
'201710',
'201711',
'201712',
'20181',
'20182',
'20183',
'20184',
'20185',
'20186',
'20187') AND z.annomesegiornodir >= 20190407 and annoquarti = ${1} and mesequarti = ${2}
	" && ./flusso-misure.sh -S2 -SMS S1 -AM_S ${1}${3} 
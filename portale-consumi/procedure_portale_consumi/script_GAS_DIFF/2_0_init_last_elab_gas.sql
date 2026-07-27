set hive.exec.dynamic.partition.mode=nonstrict;

alter table misuregas.last_elab_gas drop if exists partition (tipoflusso<>'');

insert into misuregas.last_elab_gas partition(tipoflusso)
select 19990101 annomesegiornoelab,'TML' tipoflusso  
union all
select 19990101 annomesegiornoelab,'RML' tipoflusso
union all
select 19990101 annomesegiornoelab,'VTG6' tipoflusso 
union all
select 19990101 annomesegiornoelab,'TAL' tipoflusso 
union all
select 19990101 annomesegiornoelab,'TAV' tipoflusso 
union all
select 19990101 annomesegiornoelab,'TGL' tipoflusso 
union all
select 19990101 annomesegiornoelab,'RGL' tipoflusso 
union all
select 19990101 annomesegiornoelab,'RMV' tipoflusso   ;
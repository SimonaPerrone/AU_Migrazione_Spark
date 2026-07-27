set hive.cli.print.header=false;

select annomesegiornoelab from misuregas.last_elab_gas where tipoflusso='${hiveconf:tipo_flusso}';


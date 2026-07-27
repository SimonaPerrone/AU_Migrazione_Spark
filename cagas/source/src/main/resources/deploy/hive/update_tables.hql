use ${hive.db};
msck repair table validated_flows;
msck repair table consumptions;
msck repair table ca;
msck repair table ca_pre_final;
msck repair table ca_final;
msck repair table ca_final_to_export;
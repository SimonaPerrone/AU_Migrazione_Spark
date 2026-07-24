export CREATE_VIEW_PDR=$(cat <<-EOF

CREATE OR REPLACE FORCE VIEW ${oracle.table.clg_perimetro_pdr_gm_view.db}.${oracle.table.clg_perimetro_pdr_gm_view.name}
(
   Data_Calc,
   N_Id_Pdr,
   T_Codice_Pdr,
   N_Prelievo_Annuo,
   N_Id_Remi_Anagrafica,
   T_Trattamento_Settlement,
   T_Cod_Cat_Uso,
   T_Cod_Prof
)
As
 With   D_Data_Calc
        As (Select Trunc (To_Date ('${DATA_CALC}', '${application.oracle.date.format}')) I_Data_Calc From Dual),
        D_Data_Inizio
        As (Select Add_Months(Trunc(I_Data_Calc,'MM'),1)  I_Data_Inizio From D_Data_Calc),
        N_Anno
        As (Select Case
                      When Extract (
                              Month From Trunc (D_Data_Inizio.I_Data_Inizio)) Between 10
                                                                                  And 12
                      Then
                         To_Char (
                              Extract (
                                 Year From Trunc (
                                              D_Data_Inizio.I_Data_Inizio))
                            + 1)
                      Else
                         To_Char (
                            Extract (
                               Year From Trunc (D_Data_Inizio.I_Data_Inizio)))
                   End
                      Tmp_Anno
              From D_Data_Inizio)
   Select  Dc.I_Data_Calc                           Data_Calc
          ,To_Char (Pdr.N_Id_Pdr)                   N_Id_Pdr
          ,To_Char (Pdr.T_Codice_Pdr)               T_Codice_Pdr
          ,Prel.N_Prelievo_Annuo                    N_Prelievo_Annuo
          ,To_Char (Anag_Pool.N_Id_Remi_Anagrafica) N_Id_Remi_Anagrafica
          ,Tratt.T_Trattamento_Settlement           T_Trattamento_Settlement
          ,Prof.T_Cod_Cat_Uso                       T_Cod_Cat_Uso
          ,Prof.T_Cod_Profilo                       T_Cod_Profilo
     From D_Data_Calc Dc
          Join D_Data_Inizio D On 1 = 1
          Join N_Anno N On 1 = 1
          Join Rcugas.Rcugas_Pdr Pdr On 1 = 1
          Join Rcugas.Rcugas_Pdr_Stato Pdr_Stato On Pdr_Stato.N_Id_Pdr = Pdr.N_Id_Pdr
                And Pdr_Stato.T_Cod_Stato_Pdr = 'P'
                And D.I_Data_Inizio Between Nvl(Pdr_Stato.D_Data_Inizio,To_Date ('1900','yyyy')) And Nvl (Pdr_Stato.D_Data_Fine,To_Date ('9999','yyyy'))
          Join Rcugas.Rcugas_Connessione Connessione_Remi On Connessione_Remi.N_Id_Pdr = Pdr.N_Id_Pdr
                And D.I_Data_Inizio Between Nvl(Connessione_Remi.D_Data_Inizio,To_Date ('1900','yyyy')) And Nvl ( Connessione_Remi.D_Data_Fine, To_Date ('9999','yyyy'))
          Join Rcugas.Rcugas_Remi Remi On Remi.N_Id_Remi = Connessione_Remi.N_Id_Remi
          Join Rcugas.Rcugas_Remi_Aggregazione Aggr On Aggr.N_Id_Remi_Anagrafica_Fisico = Remi.N_Id_Remi_Anagrafica
                And D.I_Data_Inizio Between  Nvl (Aggr.D_Data_Inizio,To_Date ('1900','yyyy')) And Nvl ( Aggr.D_Data_Fine, To_Date ('9999','yyyy'))
          Join Rcugas.Rcugas_Remi_Anagrafica Anag_Pool On Anag_Pool.N_Id_Remi_Anagrafica = Aggr.N_Id_Remi_Anagrafica_Pool
          Join Rcugas.Rcugas_Var_Profilo Prof On Prof.N_Id_Pdr = Pdr.N_Id_Pdr
                And To_Char (Tmp_Anno) = Prof.T_Anno
          Join Rcugas.Rcugas_Var_Trattamento Tratt On Tratt.N_Id_Pdr = Pdr.N_Id_Pdr
                And To_Char (Tmp_Anno) = Tratt.T_Anno
          Join Rcugas.Rcugas_Var_Prel_Annuo Prel On Prel.N_Id_Pdr = Pdr.N_Id_Pdr
                And To_Char (Tmp_Anno) = Prel.T_Anno
Where     1 = 1
    And D.I_Data_Inizio Between Prof.D_Data_Inizio And Nvl (Prof.D_Data_Fine, To_Date ('9999','yyyy'))
    And D.I_Data_Inizio Between Tratt.D_Data_Inizio And Nvl (Tratt.D_Data_Fine,To_Date ('9999','yyyy'))
    And D.I_Data_Inizio Between Prel.D_Data_Inizio And Nvl (Prel.D_Data_Fine, To_Date ('9999', 'YYYY'))
    And Nvl (Tratt.T_Trattamento_Settlement, 'Y') In ('G')
    And Prof.T_Cod_Cat_Uso In ('T1', 'C2')
EOF
)
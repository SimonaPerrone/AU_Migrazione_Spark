export CREATE_VIEW_REMI=$(cat <<-EOF

CREATE OR REPLACE FORCE VIEW ${oracle.table.clg_perimetro_remi_gm_view.db}.${oracle.table.clg_perimetro_remi_gm_view.name}
(
   Data_Calc
   ,Anno
   ,N_Id_Remi_Anagrafica
   ,T_Remi
   ,T_Z
   ,T_Pmax
   ,T_Cod_Profilo
)
As
   With D_Data_Calc
        As (Select Trunc (To_Date ('${DATA_CALC}', '${application.oracle.date.format}')) I_Data_Calc From Dual),
        D_Data_Inizio
        As (Select Add_Months(trunc(I_Data_Calc,'MM'),1)  I_Data_Inizio From D_Data_Calc),
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
              From D_Data_Inizio),
        Pmax_Default
        As (Select To_Number (Replace (T_Param_Val, ',', '.')) Pmax_D
              From Clg.Clg_Config_Param C Join D_Data_Inizio Di On 1 = 1
             Where     T_Cod_Classe_Param = 'CALCOLO_CTC'
                   And T_Param_Cod = 'CALC_CTC_PMX'
                   And Di.I_Data_Inizio Between D_Data_Inizio
                                            And Nvl (
                                                   D_Data_Fine,
                                                   To_Date ('9999', 'YYYY'))),
        Z_Default
        As (Select To_Number (Replace (T_Param_Val, ',', '.')) Z_D
              From Clg.Clg_Config_Param C Join D_Data_Inizio Di On 1 = 1
             Where     T_Cod_Classe_Param = 'CALCOLO_CTC'
                   And T_Param_Cod = 'CALC_CTC_Z'
                   And Di.I_Data_Inizio Between D_Data_Inizio
                                            And Nvl (
                                                   D_Data_Fine,
                                                   To_Date ('9999', 'YYYY')))
   Select Distinct
           Dc.I_Data_Calc                           Data_Calc
          ,N.Tmp_Anno                               Anno
          ,To_Char (Anag_Pool.N_Id_Remi_Anagrafica) N_Id_Remi_Anagrafica
          ,Anag_Pool.T_Remi                         T_Remi
          ,Nvl (Z.N_Z, Zd.Z_D)                      T_Z
          ,Nvl (Pmax.N_Pmax, Pd.Pmax_D)             T_Pmax
          ,Pmax.T_Cod_Prof T_Cod_Profilo
     From D_Data_Calc dc
          Join D_Data_Inizio D On 1 = 1
          Join N_Anno N On 1 = 1
          Join Pmax_Default Pd On 1 = 1
          Join Z_Default Zd On 1 = 1
          Join Rcugas.Rcugas_Remi_Anagrafica Anag_Pool On 1 = 1
          Left Join Clg.Rcugas_Remi_Z Z On     Z.N_Id_Remi_Anagrafica = Anag_Pool.N_Id_Remi_Anagrafica
                And I_Data_Inizio Between Z.D_Data_Inizio And Nvl (Z.D_Data_Fine, To_Date ('9999', 'YYYY'))
          Left Join Clg.Rcugas_Remi_Pmax Pmax On     Pmax.N_Id_Remi_Anagrafica = Anag_Pool.N_Id_Remi_Anagrafica
                And I_Data_Inizio Between Pmax.D_Data_Inizio And Nvl (Pmax.D_Data_Fine, To_Date ('9999', 'YYYY'))
    Where     1 = 1
EOF
)

# Replication of the cleaning steps from Dick-Nielsen (2009, 2014)
##########
# The original cleaning steps where performed using SAS
# Previous steps are not necessary for the data in 
# dt.TRACE.data.final the complete data 
# Comments with a leading * are directly from the original code

library(dat.table)
library(dplyr)

dt.TRACE.Post.data.final
dt.TRACE.Post.data.final[,.N, by = list(trc_st)]

#* Removes all trades with the same intra-day (and intra-bond) meassage sequence number;
#* Proc Sort Data=trace.&navnOUT nodubkey;
#* by bond_SYM_ID trd_exctn_dt MSG_SEQ_NB;
#* Run;

#* Takes out all cancellations and corrections;
#* These transactions should be deleted together with the
#* original report;
#* temp_deleteI_NEW - name in the original code and the R equivalent
dt.TRACE.Post.data.final[trc_st %in% c("X", "C"),]

# temp_deleteII_NEW - name in the original code and the R equivalent
dt.TRACE.Post.data.final[trc_st %in% c("Y"),]


# Delete the cancellations and corrections found, i.e. remove all trades from the data.table,
# which are included in dt.TRACE.Post.data.final[trc_st %in% c("X", "C"),]
# * These transactions can be matched by message sequence number
# * and date. We furthermore match on cusip, volume, price, date,
# * time, buy-sell side, contra party;
# * This is as suggested by the variable description;


# Trades to be deleted
inner_join(x = dt.TRACE.Post.data.final
           , y = dt.TRACE.Post.data.final[trc_st %in% c("X", "C"), .(cusip_id
                                                                     , entrd_vol_qt
                                                                     , rptd_pr
                                                                     , trd_exctn_dt
                                                                     , trd_exctn_tm
                                                                     , rpt_side_cd
                                                                     , cntra_mp_id
                                                                     , msg_seq_nb)]
                         
                         , by = c("cusip_id" = "cusip_id"
                                  , "entrd_vol_qt" = "entrd_vol_qt"
                                  , "rptd_pr" = "rptd_pr"
                                  , "trd_exctn_dt" = "trd_exctn_dt"
                                  , "trd_exctn_tm" = "trd_exctn_tm"
                                  , "rpt_side_cd" = "rpt_side_cd"
                                  , "cntra_mp_id" = "cntra_mp_id"
                                  , "msg_seq_nb" = "msg_seq_nb"
                         )) %>% data.table(.) 


dt.TRACE.Post.data.filtered <- dt.TRACE.Post.data.final[!dt.TRACE.Post.data.final[trc_st %in% c("X", "C"), .(cusip_id
                                                                              , entrd_vol_qt
                                                                              , rptd_pr
                                                                              , trd_exctn_dt
                                                                              , trd_exctn_tm
                                                                              , rpt_side_cd
                                                                              , cntra_mp_id
                                                                              , msg_seq_nb)],, on = .(cusip_id
                                                                                                      , entrd_vol_qt
                                                                                                      , rptd_pr
                                                                                                      , trd_exctn_dt
                                                                                                      , trd_exctn_tm
                                                                                                      , rpt_side_cd
                                                                                                      , cntra_mp_id
                                                                                                      , msg_seq_nb)]
# Original Length 9795087
# Removed Trades 9794690 means 397 Trades are removed
dt.TRACE.Post.data.filtered[,.N]

# Removal of all Reversals
dt.TRACE.Post.data.filtered[trc_st %in% c("Y"),]

inner_join(x = dt.TRACE.Post.data.filtered
           , y = dt.TRACE.Post.data.filtered[trc_st %in% c("Y"), .(cusip_id
                                                                     , entrd_vol_qt
                                                                     , rptd_pr
                                                                     , trd_exctn_dt
                                                                     , trd_exctn_tm
                                                                     , rpt_side_cd
                                                                     , cntra_mp_id
                                                                     , msg_seq_nb)]
           
           , by = c("cusip_id" = "cusip_id"
                    , "entrd_vol_qt" = "entrd_vol_qt"
                    , "rptd_pr" = "rptd_pr"
                    , "trd_exctn_dt" = "trd_exctn_dt"
                    , "trd_exctn_tm" = "trd_exctn_tm"
                    , "rpt_side_cd" = "rpt_side_cd"
                    , "cntra_mp_id" = "cntra_mp_id"
                    , "msg_seq_nb" = "msg_seq_nb"
           )) %>% data.table(.) 
# Before filtering the data 9794690
# after filtering 9789607, which means 5083 rows are removed
dt.TRACE.Post.data.filtered <- dt.TRACE.Post.data.filtered[!dt.TRACE.Post.data.filtered[trc_st %in% c("Y"), .(cusip_id
                                                                              , entrd_vol_qt
                                                                              , rptd_pr
                                                                              , trd_exctn_dt
                                                                              , trd_exctn_tm
                                                                              , rpt_side_cd
                                                                              , cntra_mp_id
                                                                              , msg_seq_nb)],, on = .(cusip_id
                                                                                                      , entrd_vol_qt
                                                                                                      , rptd_pr
                                                                                                      , trd_exctn_dt
                                                                                                      , trd_exctn_tm
                                                                                                      , rpt_side_cd
                                                                                                      , cntra_mp_id
                                                                                                      , msg_seq_nb)]











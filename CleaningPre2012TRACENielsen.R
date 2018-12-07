# Replication of the cleaning steps from Dick-Nielsen (2009)
#####
# The original cleaning steps where performed using SAS
# Previous steps are not necessary for the data in 
# dt.TRACE.data.final the complete data 
library(dat.table)
library(dplyr)



#* Delayed disseminations are treated after the same guidelines as non-delayed;
# if asof_cd='X' then asof_cd='R';
# if asof_cd='D' then asof_cd='A';

dt.TRACE.Pre.data.final
dt.TRACE.Pre.data.final[,.N, by = list(trc_st)]

dt.TRACE.Pre.data.final[asof_cd == "X", asof_cd := "R"]
dt.TRACE.Pre.data.final[asof_cd == "D", asof_cd := "A"]

#* Removes all trades with the same intra-day (and intra-bond) meassage sequence number;
#* Proc Sort Data=trace.&navnOUT nodubkey;
#* by bond_SYM_ID trd_exctn_dt MSG_SEQ_NB;
#* Run;



###
# * Takes out all cancellations into the temp_delete dataset;
#*if trc_st = ’C’ then output temp_delete;
#*temp_delete
dt.TRACE.Pre.data.final[trc_st %in% c("C", "W"),] 
#* All corrections are put into both datasets;
#* else if trc_st = ’W’ then output temp_delete temp_raw;
# 
dt.TRACE.Pre.data.final[trc_st %in% c("W"),]

# * Deletes the error trades as identified by the message
#* sequence numbers. Same day corrections and cancelations;
# Recursive join, which matches the msg_seq_nb with the orig_msg_nb and 
# thus allows to create hierarchical chains of relationships between the 
# trades, which are cancelled.
inner_join(x = dt.TRACE.Pre.data.final[is.na(orig_msg_seq_nb) ,.(cusip_id
                                                                  , msg_seq_nb
                                                                  , orig_msg_seq_nb
                                                                  , Initial.Trade.Status = trc_st
                                                                  , Initial.Trd.Rpt.Dt = trd_rpt_dt
                                                                  , Initial.Trd.Rpt.Tm = trd_rpt_tm
                                                                  , Initial.Trd.Exctn.Dt = trd_exctn_dt
                                                                  , Initial.Trd.Exctn.Tm = trd_exctn_tm
 )]
 , y = dt.TRACE.Pre.data.final[trc_st %in% c("C", "W") ,.(cusip_id
                                   , J2.Msg.Nb = msg_seq_nb
                                   , orig_msg_seq_nb
                                   , J2.Trade.Status = trc_st
                                   , J2.Trd.Rpt.Dt = trd_rpt_dt
                                   , J2.Trd.Rpt.Tm = trd_rpt_tm
                                   , J2.Trd.Exctn.Dt = trd_exctn_dt
                                   , J2.Trd.Exctn.Tm = trd_exctn_tm)]
 
 , by = c("cusip_id" = "cusip_id"
          ,"msg_seq_nb" = "orig_msg_seq_nb"
          , "Initial.Trd.Rpt.Dt" = "J2.Trd.Rpt.Dt")) %>% data.table(.) %>% { . ->> dt.join.1}  %>% filter(., is.na(orig_msg_seq_nb)) %>% inner_join(
                  x = .
                  , y = dt.TRACE.Pre.data.final[trc_st %in% c("C", "W") ,.(cusip_id
                                                    , J3.Msg.Nb = msg_seq_nb
                                                    , orig_msg_seq_nb
                                                    , J3.Trade.Status = trc_st
                                                    , J3.Trd.Rpt.Dt = trd_rpt_dt
                                                    , J3.Trd.Rpt.Tm = trd_rpt_tm
                                                    , J3.Trd.Exctn.Dt = trd_exctn_dt
                                                    , J3.Trd.Exctn.Tm = trd_exctn_tm)]
                  , by = c( "cusip_id" = "cusip_id"
                            , "J2.Msg.Nb" = "orig_msg_seq_nb"
                            , "Initial.Trd.Rpt.Dt" = "J3.Trd.Rpt.Dt")) %>%  data.table(.)%>% { . ->> dt.join.2} %>% filter(., is.na(orig_msg_seq_nb)) %>%  inner_join(
                                    x = .
                                    , y = dt.TRACE.Pre.data.final[trc_st %in% c("C", "W") ,.(cusip_id
                                                                      , J4.Msg.Nb = msg_seq_nb
                                                                      , orig_msg_seq_nb
                                                                      , J4.Trade.Status = trc_st
                                                                      , J4.Trd.Rpt.Dt = trd_rpt_dt
                                                                      , J4.Trd.Rpt.Tm = trd_rpt_tm
                                                                      , J4.Trd.Exctn.Dt = trd_exctn_dt
                                                                      , J4.Trd.Exctn.Tm = trd_exctn_tm)]
                                    , by = c( "cusip_id" = "cusip_id"
                                              , "J3.Msg.Nb" = "orig_msg_seq_nb"
                                              , "Initial.Trd.Rpt.Dt" = "J4.Trd.Rpt.Dt")) %>% data.table(.) %>% { . ->> dt.join.3} %>% filter(., is.na(orig_msg_seq_nb)) %>% inner_join( 
                                                      x = .
                                                      , y = dt.TRACE.Pre.data.final[trc_st %in% c("C", "W") ,.(cusip_id
                                                                                        , J5.Msg.Nb = msg_seq_nb
                                                                                        , orig_msg_seq_nb
                                                                                        , J5.Trade.Status = trc_st
                                                                                        , J5.Trd.Rpt.Dt = trd_rpt_dt
                                                                                        , J5.Trd.Rpt.Tm = trd_rpt_tm
                                                                                        , J5.Trd.Exctn.Dt = trd_exctn_dt
                                                                                        , J5.Trd.Exctn.Tm = trd_exctn_tm)]
                                                      , by = c("cusip_id" = "cusip_id"
                                                               , "J4.Msg.Nb" = "orig_msg_seq_nb"
                                                               , "Initial.Trd.Rpt.Dt" = "J5.Trd.Rpt.Dt")) %>% data.table(.) %>% { . ->> dt.join.4} %>% filter(., is.na(orig_msg_seq_nb)) %>% inner_join(
                                                                       x = .
                                                                       , y = dt.TRACE.Pre.data.final[trc_st %in% c("C", "W") ,.(cusip_id
                                                                                                         , J6.Msg.Nb = msg_seq_nb
                                                                                                         , orig_msg_seq_nb
                                                                                                         , J6.Trade.Status = trc_st
                                                                                                         , J6.Trd.Rpt.Dt = trd_rpt_dt
                                                                                                         , J6.Trd.Rpt.Tm = trd_rpt_tm
                                                                                                         , J6.Trd.Exctn.Dt = trd_exctn_dt
                                                                                                         , J6.Trd.Exctn.Tm = trd_exctn_tm)]
                                                                       , by = c("cusip_id" = "cusip_id"
                                                                                , "J5.Msg.Nb" = "orig_msg_seq_nb"
                                                                                , "Initial.Trd.Rpt.Dt" = "J6.Trd.Rpt.Dt")) %>% data.table(.)%>% { . ->> dt.join.5}%>%  filter(., is.na(orig_msg_seq_nb)) %>% inner_join( 
                                                                                        x = .
                                                                                        , y = dt.TRACE.Pre.data.final[trc_st %in% c("C", "W") ,.(cusip_id
                                                                                                                          , J7.Msg.Nb = msg_seq_nb
                                                                                                                          , orig_msg_seq_nb
                                                                                                                          , J7.Trade.Status = trc_st
                                                                                                                          , J7.Trd.Rpt.Dt = trd_rpt_dt
                                                                                                                          , J7.Trd.Rpt.Tm = trd_rpt_tm
                                                                                                                          , J7.Trd.Exctn.Dt = trd_exctn_dt
                                                                                                                          , J7.Trd.Exctn.Tm = trd_exctn_tm)]
                                                                                        , by = c("cusip_id" = "cusip_id"
                                                                                                 ,"J6.Msg.Nb" = "orig_msg_seq_nb"
                                                                                                 , "Initial.Trd.Rpt.Dt" = "J7.Trd.Rpt.Dt")) %>% data.table(.) %>% { . ->> dt.join.6} %>%  filter(., is.na(orig_msg_seq_nb)) %>% inner_join( 
                                                                                                         x = .
                                                                                                         , y = dt.TRACE.Pre.data.final[trc_st %in% c("C", "W") ,.(cusip_id
                                                                                                                                           , J8.Msg.Nb = msg_seq_nb
                                                                                                                                           , orig_msg_seq_nb
                                                                                                                                           , J8.Trade.Status = trc_st
                                                                                                                                           , J8.Trd.Rpt.Dt = trd_rpt_dt
                                                                                                                                           , J8.Trd.Rpt.Tm = trd_rpt_tm
                                                                                                                                           , J8.Trd.Exctn.Dt = trd_exctn_dt
                                                                                                                                           , J8.Trd.Exctn.Tm = trd_exctn_tm)]
                                                                                                         , by = c("cusip_id" = "cusip_id"
                                                                                                                  ,"J7.Msg.Nb" = "orig_msg_seq_nb"
                                                                                                                  , "Initial.Trd.Rpt.Dt" = "J8.Trd.Rpt.Dt")) %>% data.table(.) %>% { . ->> dt.join.7} 
 
 
 
 
 
 # Rbindlist all the joined data.tables and then select all messages, which are ultimately deleted
 # and filter them out of the original final TRACE dataset
 
 dt.join.final.data <- rbindlist(list(  dt.join.1
                                        , dt.join.2
                                        , dt.join.3
                                        , dt.join.4
                                        , dt.join.5
                                        , dt.join.6
                                        , dt.join.7
)
                                 , fill = TRUE
                                 , idcol = "id"
                                 , use.names = TRUE)
 
 rm(  dt.join.1
      , dt.join.2
      , dt.join.3
      , dt.join.4
      , dt.join.5
      , dt.join.6
      , dt.join.7
)

 # Select all rows, where in any of the trade status columns there's a C 
 colnames.status <- as.vector(unlist(data.table(Colnames = colnames(dt.join.final.data))[like(Colnames, "*Trade.Status")]))
 
 # Check the correct filtering
 # dt.join.final.data[dt.join.final.data[, Reduce(`|`, lapply(.SD, `%in%`, c("C", "R"))),.SDcols = colnames.status[1:2]],]
 # dt.join.final.data[Initial.Trade.Status %in% c("C", "R")| J2.Trade.Status %in% c("C", "R"),  ]
 
 
 dt.join.final.data[dt.join.final.data[, Reduce(`|`, lapply(.SD, `%in%`, c("C"))),.SDcols = colnames.status], .(cusip_id
                                                                                                                , msg_seq_nb
                                                                                                                , Initial.Trd.Rpt.Dt
                                                                                                                , J2.Msg.Nb
                                                                                                                , J3.Msg.Nb
                                                                                                                , J4.Msg.Nb
                                                                                                                , J5.Msg.Nb
                                                                                                                , J6.Msg.Nb
                                                                                                                , J7.Msg.Nb
                                                                                                                , J8.Msg.Nb
 )]
 
 dt.unique.cancelled.msg.nb <- unique(dt.join.final.data[dt.join.final.data[, Reduce(`|`, lapply(.SD, `%in%`, c("C"))),.SDcols = colnames.status], .(cusip_id
                                                                                                                                                     , msg_seq_nb
                                                                                                                                                     , Initial.Trd.Rpt.Dt
                                                                                                                                                     , J2.Msg.Nb
                                                                                                                                                     , J3.Msg.Nb
                                                                                                                                                     , J4.Msg.Nb
                                                                                                                                                     , J5.Msg.Nb
                                                                                                                                                     , J6.Msg.Nb
                                                                                                                                                     , J7.Msg.Nb
                                                                                                                                                     , J8.Msg.Nb
 )])
 
 
 dt.Trade.Deletion.List <- melt(dt.unique.cancelled.msg.nb, id.vars = c("cusip_id","Initial.Trd.Rpt.Dt")
                                , measure.vars = c( "msg_seq_nb"
                                                    , "J2.Msg.Nb"
                                                    , "J3.Msg.Nb"
                                                    , "J4.Msg.Nb"
                                                    , "J5.Msg.Nb"
                                                    , "J6.Msg.Nb"
                                                    , "J7.Msg.Nb"
                                                    , "J8.Msg.Nb")
 )[!is.na(value),] 
 
 
 
 # Filter out the trades
 merge(x = dt.TRACE.Pre.data.final[trc_st != "R"]
       , y = dt.Trade.Deletion.List
       , by.x = c("cusip_id", "trd_rpt_dt", "msg_seq_nb")
       , by.y = c("cusip_id", "Initial.Trd.Rpt.Dt", "value")
       , all = FALSE)[,.N]
 202773
 dt.TRACE.Pre.data.final[,.N]
 6843141
 
 dt.Trades.to.Remove <-  unique(inner_join(dt.TRACE.Pre.data.final[trc_st != "R"]
                                           , dt.Trade.Deletion.List[,.(cusip_id 
                                                                       , Initial.Trd.Rpt.Dt
                                                                       , value)]
                                           , by = c("cusip_id" = "cusip_id"
                                                    , "trd_rpt_dt" = "Initial.Trd.Rpt.Dt"
                                                    , "msg_seq_nb" = "value"
                                           )) %>% data.table(.))
 
 
 
 # Initial number of trades: 6843141 
 # After filtering: 6640368
 # Removal of 202773
 
 dt.TRACE.Pre.data.filtered <- dt.TRACE.Pre.data.final[!dt.Trades.to.Remove, on = .(cusip_id, trd_rpt_dt, msg_seq_nb)]
 
 
 #* reversal dataset
 #* temp_raw3 - i.e. the dataset without the Reversals
 # Total dataset: 6669122
 # Data without reversals: 6589857
 # Reversals: 79265
 dt.TRACE.Pre.data.filtered[!(asof_cd %in% c("R")),.( trd_exctn_dt
                                     , cusip_id 
                                     , trd_exctn_tm
                                     , rptd_pr
                                     , entrd_vol_qt
                                     , rpt_side_cd
                                     , cntra_mp_id
                                     , trd_rpt_dt
                                     , trd_rpt_tm
                                     , msg_seq_nb 
                                 )]
 
 # reversal data
 dt.TRACE.Pre.data.filtered[asof_cd %in% c("R"), .( trd_exctn_dt
                                                   , cusip_id 
                                                   , trd_exctn_tm
                                                   , rptd_pr
                                                   , entrd_vol_qt
                                                   , rpt_side_cd
                                                   , cntra_mp_id
                                                   , trd_rpt_dt
                                                   , trd_rpt_tm
                                                   , msg_seq_nb 
                                                   )]
 
 
 #* merge temp_raw3 (in=qqq) reversal (in=qq) ;
 #* by trd_exctn_dt cusip_id trd_exctn_tm rptd_pr
 #* entrd_vol_qt rpt_side_cd cntra_mp_id;
 # https://communities.sas.com/t5/New-SAS-User/Understanding-SAS-code-and-translate-it-into-R-code/m-p/519108#M3720
 # reversal2 in the original code
 tmp.dt.reversal <- inner_join( x =  dt.TRACE.Pre.data.filtered[!(asof_cd %in% c("R")),.( trd_exctn_dt
                                                                  , cusip_id 
                                                                  , trd_exctn_tm
                                                                  , rptd_pr
                                                                  , entrd_vol_qt
                                                                  , rpt_side_cd
                                                                  , cntra_mp_id
                                                                  , trd_rpt_dt
                                                                  , trd_rpt_tm
                                                                  , msg_seq_nb 
                                                                  , trc_st
                                                                  , asof_cd
 )]
 , y =  dt.TRACE.Pre.data.filtered[asof_cd %in% c("R"), .( trd_exctn_dt
                                                           , cusip_id 
                                                           , bond_sym_id
                                                           , trd_exctn_tm
                                                           , rptd_pr
                                                           , entrd_vol_qt
                                                           , rpt_side_cd
                                                           , cntra_mp_id
                                                           , trd_rpt_dt
                                                           , trd_rpt_tm
                                                           , msg_seq_nb 
                                                           , trc_st
                                                           , asof_cd
 )]
 , by = c("trd_exctn_dt"
          , "cusip_id"
          , "trd_exctn_tm"
          , "rptd_pr"
          , "entrd_vol_qt"
          , "rpt_side_cd"
          , "cntra_mp_id")
        
        ) %>% data.table(.)
 
 # the reporting date for the reversal lies after the reporting date for the original
 # report
 tmp.dt.reversal <- tmp.dt.reversal[trd_rpt_dt.x < trd_rpt_dt.y]
 
 
 #*proc sort data=reversal2 nodupkey; by trd_exctn_dt bond_sym_id
 #*trd_exctn_tm rptd_pr entrd_vol_qt; run
 
 tmp.dt.reversal <- tmp.dt.reversal[!duplicated(tmp.dt.reversal, by = c("trd_exctn_dt"
                                                                       , "trd_exctn_tm"
                                                                        , "cusip_id"
                                                                        , "rptd_pr"
                                                                        , "entrd_vol_qt")
                                                                        )]
                         
 
 
 # Remove the reversals from the filtered data.table
 #*on trd_exctn_dt, bond_sym_id, trd_exctn_tm, rptd_pr, entrd_vol_qt
 #* Deletes the macthing reversals;
 #*data temp_raw4;
 #*merge
 #* reversal2 (in=qq)
 #* temp_raw3
 #* ;
 #* by N;
 #* if not qq;
 #* run;
 # 
 # Find all the trades which are in tmp.dt.reversal and thus can be removed from the original dataset
 
 inner_join(x =  dt.TRACE.Pre.data.filtered[!(asof_cd %in% c("R")),]
                , y =  tmp.dt.reversal
            , by = c("trd_exctn_dt"
                     , "trd_exctn_tm"
                     , "cusip_id"
                     , "rptd_pr"
                     , "entrd_vol_qt"
                     , "rpt_side_cd"
                     , "cntra_mp_id")
            ) %>% data.table(.)
 
 
 dt.TRACE.Pre.data.filtered[!(asof_cd %in% c("R")),][ !(tmp.dt.reversal) , , on = .(cusip_id
                                                                                 , entrd_vol_qt
                                                                                 , rptd_pr
                                                                                 , trd_exctn_dt
                                                                                 , trd_exctn_tm
                                                                                 , rpt_side_cd
                                                                                 , cntra_mp_id
                                                                                 ) ]

 
 dt.TRACE.Pre.data.filtered <- dt.TRACE.Pre.data.filtered[!(asof_cd %in% c("R")),][ !(tmp.dt.reversal) , , on = .(cusip_id
                                                                                    , entrd_vol_qt
                                                                                    , rptd_pr
                                                                                    , trd_exctn_dt
                                                                                    , trd_exctn_tm
                                                                                    , rpt_side_cd
                                                                                    , cntra_mp_id
 ) ]
 
 dt.TRACE.Pre.data.filtered[,.N, by = list(trc_st)]

 # Combine Pre and Post data
 
 dt.TRACE.data.final.filtered <- rbindlist(list(dt.TRACE.Pre.data.filtered
                                                , dt.TRACE.Post.data.filtered))
 
 
 
saveRDS(dt.TRACE.data.final.filtered, file = "TRACE_Data_Final_Filtered.RDS", compress = "xz")
 
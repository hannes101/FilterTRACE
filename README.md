# FilterTRACE
This is an effort to port the SAS code provided by Dick-Nielsen (2009, 2014) to R. The filtering steps are applied to data from the enhanced Trade Reporting and Compliance Engine (TRACE) as reported to the FINRA. The original data was downloaded from WRDS.
It is necessary to split the sample in two, since on 6th February 2012 there were changes to the database format and the way the trades were reported.
Thus, the filtering steps are described in two separate R-files describing the filtering for the two different samples.
The filtering algorithm for the pre-2012 period is a bit more elaborate than the Dick-Nielsen algorithm, since it accounts for more recursive matches between corrected trades. Thus it should result in more trades to be removed.
Help was provided by the SAS support forums.
This is work for the following research project analyzing the development of cost of debt in the oil industry:
http://www.cmstatistics.org/RegistrationsV2/CMStatistics2018/viewSubmission.php?in=1433&token=q2s44176ro80nr027s67s79418858qsq

Dick-Nielsen, Jens, Liquidity Biases in TRACE (June 3, 2009). Journal of Fixed Income, Vol. 19, No. 2, 2009. Available at SSRN: https://ssrn.com/abstract=1424870 or http://dx.doi.org/10.2139/ssrn.1424870 

Dick-Nielsen, Jens, How to Clean Enhanced TRACE Data (December 3, 2014). Available at SSRN: https://ssrn.com/abstract=2337908 or http://dx.doi.org/10.2139/ssrn.2337908 
 

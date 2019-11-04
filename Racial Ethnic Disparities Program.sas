/* Delta HRRP program to look at the impacts of the new payment reform on the quality of care 
for patients in the delta region. 

Modification of original plan to add a DiD analysis for the project. As such, the 2018 data from the HRRP 
must be added to the data set and the necessary covariates included as well. 

Robert Schuldt
10-23-2019
*/

/* bring in the data set */

proc import datafile ='***************** Stack Data\2018_HRRP_full.dta'
DBMS = DTA out = hrrp replace;
run;
/*Rename the variables to match the other data set that I have worked with. */
data hrrp2018;
	set hrrp;
	rename Acute_Myocardial_Infarction_Exce  = ERR_for_AMI;
	rename Chronic_Obstructive_Pulmonary_Di = ERR_for_COPD; 
	rename Excess_Readmission_Ratio_for_Hea = ERR_for_HF; 
	rename Excess_Readmission_Ratio_for_Pne = ERR_for_Pneumonia; 
	rename Coronary_Artery_Bypass_Graft_Exc = ERR_for_CABG;
	rename Hip_Knee_Arthroplasty_Excess_Rea = ERR_for_THA_TKA;
	rename Number_of_Acute_Myocardial_Infar = f0;
	rename Number_of_Chronic_Obstructive_Pu = f1;
	rename Number_of_Heart_Failure_Cases = f2;
	rename Number_of_Pneumonia_Cases = f3;
	rename Number_of_Coronary_Artery_Bypass = f4;
	rename Number_of_Hip_Knee_Arthroplasty_ = f5;





	prct_penalty= 1 - FY_2018_Readmissions_Adjustment_;

	penalty = 0;
	 if FY_2018_Readmissions_Adjustment_ < 1 then penalty = 1;
	


run;
/* I need to bring the POS data for identifying hospital types*/
libname pos '******************lth_Policy_Management\Data\POS\2015\Data';

data pos_data;
	set pos.pos_2015;
	keep PRVDR_NUM FIPS_STATE_CD FIPS_CNTY_CD CRTFD_BED_CNT MDCL_SCHL_AFLTN_CD  GNRL_CNTL_TYPE_CD Provider;
		where PRVDR_CTGRY_CD = '01';
		rename PRVDR_NUM = Provider;
	run;

/* want to bring in my small sorting macro*/
%include '********************esearch Work\SAS Macros\infile macros\sort.sas';

%sort(pos_data, provider)
%sort(hrrp2018, provider)
/*Now I will merge with the HRRP file*/

data hrrp_pos;
	merge hrrp2018 (in = a) pos_data ( in = b);
	by provider;
	if a;
	if b;
	run; /* we lose 15 observations*/
/* check what the ownership types are*/
proc freq data = hrrp_pos;
table GNRL_CNTL_TYPE_CD;
title ' Ownership Classes';
run; 

%let own = GNRL_CNTL_TYPE_CD;

data prep_set;
	set hrrp_pos;
	if &own = "04" then fp = 1;
		else fp = 0;
	if &own = '01' or &own = "02" or &own = "03" 
		then nfp = 1;
			else nfp = 0;
	gov = 0;
	if fp ne 1 and nfp ne 1 then gov = 1;

	/*need to make the fips*/
	length fips $ 5;
	fips = cats('' ,FIPS_STATE_CD, FIPS_CNTY_CD);

	 lsize=(CRTFD_BED_CNT>=300);

	 year = 2018;

	  if MDCL_SCHL_AFLTN_CD=1 then major_teach= 1; 
			else major_teach=0;

	 if MDCL_SCHL_AFLTN_CD=2 or MDCL_SCHL_AFLTN_CD=3 then minor_teach= 1; 
			else minor_teach=0;

		if fips = "12025" then fips = '12086';
		if fips = '51515' then fips = '51019';
	if fips = '02120' then fips = '02122'; 

run;
libname ahrf '************************icy_Management\Data\AHRF\2017-2018';

/*Now need to bring in the AHRF data to complete the data set*/
data ahrf_data;
	set ahrf.ahrf_2017_2018;
	keep fips poverty homehealthagen pop_estimate hosp_beds_per_cap snf_beds_per_cap pcp_per_cap pct_black unemployment;
	fips = f00002;
	poverty = F1332115;
	homehealthagen = F1321415;
	
	pop_estimate = F1198415;
	
	pcp_per_cap = (F1467515/F1198415)*1000;
	hosp_beds_per_cap = (F0892115/F1198415)*1000;
	snf_beds_per_cap = (F1321315/F1198415)*1000;

	pct_black = (( F1391015+ F1391115)/ F1198415)*100;
	pct_hisp = (( F1392015 +   F1392115)/  F1198415)*100;
	unemployment = F0679515;


run; 


%sort(ahrf_data, fips)
%sort(prep_set, fips)

data hrrp2018;
	merge prep_set (in = a) ahrf_data (in = b);
	by fips;
	if a;

run;

proc import datafile = '************************k Data\delta_sep9_2019.dta' 
dbms = dta out = hrrp2019 (rename =( Hospital_CCN = Provider)) replace; 
run;

data hrrp_2019;
	set hrrp2019;
	keep Provider Peer_Group_Assignment;
run;


%sort(hrrp_2019, Provider)
%sort(hrrp2018, Provider)

data hrrp_peer;
	merge hrrp2018 (in = a) hrrp_2019 (in = b);

	by provider;
	if a;
run;
/*NOTE: There were 3399 observations read from the data set WORK.HRRP2018.
NOTE: There were 3173 observations read from the data set WORK.HRRP_2019
*/

libname delta '*****************RRP';
/*Need to bring in delta county identifier*/
data counties;
	set delta.delta_counties;
	delta = 1;

	length fips $ 5;

	fips = put(FIPS_num, z5.);
run;


%sort(counties, fips)
%sort(hrrp_peer, fips)

data hrrp_2018;
merge hrrp_peer (in = a) counties (in = b);
by fips;
if a;
run;

libname final '****************HRRP\10-29-2019';

data final.hrrp_18;
	set hrrp_2018;

	if delta = . then delta = 0;
	payment_adjustment = FY_2018_Readmissions_Adjustment_;
	run;

/*do the same for 2019 data so everything is the exact same should have written this as a macro, but would take too long to modify
	will just be easy to copy/paste the code and run for 2019*/

proc import datafile= '*******************leaned file.xlsx'
dbms = XLSX out = hrrp replace;
run;
data hrrp2019pt2;
	set hrrp;
	rename ERR_for_AMI   = ERR_for_AMI;
	rename ERR_for_COPD  = ERR_for_COPD; 
	rename ERR_for_HF  = ERR_for_HF; 
	rename ERR_for_Pneumonia  = ERR_for_Pneumonia; 
	rename ERR_for_CABG  = ERR_for_CABG;
	rename ERR_for_THA_TKA  = ERR_for_THA_TKA;
	rename Number_of_Eligible_Discharges_fo = f0;
	rename Number_of_Eligible_Discharges_f1 = f1;
	rename Number_of_Eligible_Discharges_f2 = f2;
	rename Number_of_Eligible_Discharges_f3 = f3;
	rename Number_of_Eligible_Discharges_f4 = f4;
	rename Number_of_Eligible_Discharges_f5 = f5;
	rename Hospital_CCN = provider;


	prct_penalty= 1 - __Payment_Adjustment_Factor;

	 if __Payment_Adjustment_Factor <1 then penalty = 1;
	 	else penalty = 0;



run;

libname pos '*******************licy_Management\Data\POS\2016\Data';

data pos_data;
	set pos.pos_2016;
	keep PRVDR_NUM FIPS_STATE_CD FIPS_CNTY_CD CRTFD_BED_CNT MDCL_SCHL_AFLTN_CD  GNRL_CNTL_TYPE_CD Provider;
		where PRVDR_CTGRY_CD = '01';
		rename PRVDR_NUM = Provider;
	run;


	
/* want to bring in my small sorting macro*/
%include '********************huldt Research Work\SAS Macros\infile macros\sort.sas';

%sort(pos_data, provider)
%sort(hrrp2019pt2, provider)
/*Now I will merge with the HRRP file*/

data hrrp_pos;
	merge hrrp2019pt2 (in = a) pos_data ( in = b);
	by provider;
	if a;
	if b;
	run; /* we lose 15 observations*/
/* check what the ownership types are*/
proc freq data = hrrp_pos;
table GNRL_CNTL_TYPE_CD;
title ' Ownership Classes';
run; 

%let own = GNRL_CNTL_TYPE_CD;

data prep_set;
	set hrrp_pos;
	if &own = "04" then fp = 1;
		else fp = 0;
	if &own = '01' or &own = "02" or &own = "03" 
		then nfp = 1;
			else nfp = 0;
	gov = 0;
	if fp ne 1 and nfp ne 1 then gov = 1;

	/*need to make the fips*/
	length fips $ 5;
	fips = cats('' ,FIPS_STATE_CD, FIPS_CNTY_CD);

	 lsize=(CRTFD_BED_CNT>=300);

	 year = 2019;

	  if MDCL_SCHL_AFLTN_CD=1 then major_teach= 1; 
			else major_teach=0;

	 if MDCL_SCHL_AFLTN_CD=2 or MDCL_SCHL_AFLTN_CD=3 then minor_teach= 1; 
			else minor_teach=0;

			if fips = '02120' then fips = '02122';

run;
libname ahrf '******************nt\Data\AHRF\2017-2018';

/*Now need to bring in the AHRF data to complete the data set*/
data ahrf_data;
	set ahrf.ahrf_2017_2018;
	keep fips poverty homehealthagen pop_estimate hosp_beds_per_cap snf_beds_per_cap pcp_per_cap pct_black unemployment;
	fips = f00002;
	poverty = F1332116;
	homehealthagen = F1321416;
	
	pop_estimate = F1198416;
	
	pcp_per_cap = (F1467516/F1198416)*1000;
	hosp_beds_per_cap = (F0892116/F1198416)*1000;
	snf_beds_per_cap = (F1321316/F1198416)*1000;

	pct_black = (( F1391016+ F1391116)/ F1198416)*100;
	pct_hisp = (( F1392016 +   F1392116)/  F1198416)*100;


	unemployment = F0679516;


run; 


%sort(ahrf_data, fips)
%sort(prep_set, fips)

data hrrp2019;
	merge prep_set (in = a) ahrf_data (in = b);
	by fips;
	if a;

run;

/*NOTE: There were 3399 observations read from the data set WORK.HRRP2018.
NOTE: There were 3173 observations read from the data set WORK.HRRP_2019
*/

libname delta 'Z:\DATA\HRRP';
/*Need to bring in delta county identifier*/
data counties;
	set delta.delta_counties;
	delta = 1;

	length fips $ 5;

	fips = put(FIPS_num, z5.);
run;


%sort(counties, fips)
%sort(hrrp2019, fips)

data hrrp_2019;
merge hrrp2019 (in = a) counties (in = b);
by fips;
if a;
run;

libname final 'Z:\DATA\HRRP\10-29-2019';

data final.hrrp_19;
	set hrrp_2019;

	if delta = . then delta = 0;
	payment_adjustment = __Payment_Adjustment_Factor;

	run;

data final.full_set_hhrp;
	set
final.hrrp_18
final.hrrp_19;

drop FY_2018_Readmissions_Adjustment_  aj aj ak am al ao ap aq as at au av aw ax ay az ba bb bc bd be bf bg bh __Payment_Adjustment_Factor;
	if f0 >= 25 then qf_ami = 1; else qf_ami = 0;
	if f2 >= 25 then qf_hf = 1; else qf_hf = 0;
	if f3 >= 25 then qf_pn = 1; else qf_pn = 0;
	if f1 >= 25 then qf_copd = 1; else qf_copd = 0;
	if f4 >= 25 then qf_cabg = 1; else qf_cabg = 0;
	if f5 >= 25 then qf_tha = 1; else qf_tha = 0;	

		totcond=sum (qf_ami, qf_hf, qf_pn, qf_copd, qf_cabg, qf_tha);

	run;

	data test_fips;
		set final.full_set_hhrp;
		if fips = '02122' or fips = '12086' or fips = '51019';
	run;

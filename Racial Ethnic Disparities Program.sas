/*** This is for the research project investigating the impact of Pay-for-performance on*** 
racial/ethnic health disparities. Using IPPS, Hospital Compare, and AHRF. 

Program written by Robert Schuldt E-Mail: rschuldt@uams.edu

*****************************************12/19/2018****************************************/

libname output "E:\HRRP\12 19 2018";
libname delta "E:\HRRP";
libname ahrf "\\FileSrv1\CMS_Caregiver\DATA\HRRP\AHRF";

/* Macro for easy sorting */
%macro sort(dataset, sorted);
proc sort data = &dataset;
by &sorted;
run;

%mend sort;

/*2019 IPPS Final Rule HRRP data set variable acquisition*/

proc import datafile = "E:\HRRP\ipps2019"
dbms= xlsx out = ipps replace;
run;
/* The ERR variables are character variables, but we need them to be numeric, this process replaces them as numeric 
with cleaner variable names for future analysis as well */
data ipps_clean (rename = (__Payment_Adjustment_Factor = Payment_adjust )) ;
set ipps; 
keep hosp_num   Peer_Group_Assignment Dual_Proportion __Payment_Adjustment_Factor AMI COPD HF Pneumonia CABG THA_TKA f0 f1 f2 f3 f4 f5;
	
	array err (12) ERR_for_AMI ERR_for_COPD ERR_for_HF ERR_for_Pneumonia ERR_for_CABG ERR_for_THA_TKA Number_of_Eligible_Discharges_fo Number_of_Eligible_Discharges_f1 
				  Number_of_Eligible_Discharges_f2 Number_of_Eligible_Discharges_f3 Number_of_Eligible_Discharges_f4 Number_of_Eligible_Discharges_f5 ;
	array new (12) AMI COPD HF Pneumonia CABG THA_TKA f0 f1 f2 f3 f4 f5 ;

		do i = 1 to 12;
			new(i) = input(err(i), 11.);

		end;
		hosp_num = input(hospital_ccn, 6.);
run;
/*Creates dataset that ranked vars will be merged to*/

data output.ipps_ranked1 (rename = (hosp_num = hospital_ccn ))  ;
	set ipps_clean;
	
	
run;
/*Macro function to create quartiles of ERR measurements. In order to prevent overwriting I needed to merge after each run of the macro. 
 will look into how to incorporate directly into the macro itself when I have more time*/

%macro rank(measure, n);
proc rank  data = output.ipps_ranked out = ipps_&measure groups = 4;
	var &measure;
	where f&n ge 24;
	ranks rank&measure;
run;
 
data ipps_merge&measure;
	set ipps_&measure;
		if rank&measure = 0 then top_&measure = 1;
			else top_&measure = 0;
	run;

%mend rank;

%rank(AMI, 0)
data output.ipps_ranked2;
	merge output.ipps_ranked1 (in = a) ipps_mergeAMI (in = b);
		by Hospital_CCN;
		if a;
		
run;
%rank(COPD, 1)
data output.ipps_ranked3;
	merge output.ipps_ranked2 (in = a) ipps_mergeCOPD (in = b);
		by Hospital_CCN;
		if a;
		
run;
%rank(HF, 2)
data output.ipps_ranked4;
	merge output.ipps_ranked3 (in = a) ipps_mergeHF (in = b);
		by Hospital_CCN;
		if a;
		
run;
%rank(Pneumonia, 3)
data output.ipps_ranked5;
	merge output.ipps_ranked3 (in = a) ipps_mergePneumonia (in = b);
		by Hospital_CCN;
		if a;
		
run;
%rank(CABG, 4)
data output.ipps_ranked6;
	merge output.ipps_ranked5 (in = a) ipps_mergeCABG (in = b);
		by Hospital_CCN;
		if a;
		
run;
%rank(THA_TKA, 5)
data output.ipps_ranked7;
	merge output.ipps_ranked6 (in = a) ipps_mergeTHA_TKA (in = b);
		by Hospital_CCN;
		if a;
		
run;

/* Now we bring in the hospital general information data from revised 2016 flat files 
Sourced : https://data.medicare.gov/data/archives/hospital-compare                  */

proc import datafile = "E:\HRRP\Hospital General Information"
dbms= xlsx out = hosp replace;
run;

data output.hospital_info;
	set hosp;
	keep Hospital_CCN Hospital_Ownership gov fp nfp;
	if find(Hospital_Ownership, 'Government') > 0  then gov = 1; 
		else gov = 0; 

	if find(Hospital_Ownership, 'Proprietary') > 0  then fp = 1;
		else fp = 0; 

	if find(Hospital_Ownership, 'Voluntary') > 0  then nfp = 1;
		else nfp = 0; 

run;
/*Sorting the data to allow for a merge*/
%sort(output.ipps_ranked7, hospital_ccn)
%sort(output.hospital_info, hospital_ccn)

data hosp_ipps;
merge output.ipps_ranked7 (in = a) output.hospital_info (in = b);
by hospital_ccn;
if a;
if b;
run;

/* The next data set has character CCN numbers*/

data hosp_ippsmerge;
set hosp_ipps;
	hospital_num = put(hospital_ccn, $23.);
	hospital_num = compress(hospital_num);
run;
/* Hospital 2016 Provider of service file 
Sourced:https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Provider-of-Services/POS2015.html */

proc import datafile = "E:\HRRP\pos_file"
dbms= xlsx out = pos replace;
run;

proc means data = pos mean clm p75;
var CRTFD_BED_CNT;
run;

data output.hosp_pos;
	set pos;
	keep PRVDR_CTGRY_CD  hospital_num FIPS_STATE_CD FIPS_CNTY_CD CRTFD_BED_CNT MDCL_SCHL_AFLTN_CD;
		where PRVDR_CTGRY_CD = 1;

/* When I created the character variable is had a huge number of spaces. This cleans up the spaces so we can merge*/
		hospital_num = compress(Hospital_CCN);
	run;
%sort(hosp_ippsmerge, hospital_num)
%sort(output.hosp_pos, hospital_num)
/*merging the two sets together*/
data hosp_posmerge;
merge  hosp_ippsmerge (in = a) output.hosp_pos (in = b);
by hospital_num;
if a;
if b;
run;
proc means data = hosp_posmerge  mean clm p75;;
var CRTFD_BED_CNT;
run;


%macro fips(set, state, count);

data hospital_fips; /*Chose whatever name you please*/
	set &set;
	drop single;

/*For No Leading Zero States*/
	length state_code $ 2;
	state_code = "  ";
		/* State FIPS with both digits*/

	if &state ge 10 then state_code = &state;
/**IF YOU ARE NOT USING THE TWO DIGIT STATE DELETE THE ABOVE LINE AND SUBSTITUTE THE FOLLOWING:
	state_code = &state;
***************************************************************************************/
	length single $ 2;
	single = "  ";
		if &state lt 10 then single = &state;
		if state_code = '' then state_code = put(input(single, best2.),z2.);
		single ="";
/*********************************end of fix for FIPS STATE CODE*********************/

/* Fix the FIPS county code with two and one digit***********************************/
	length county_code $ 3;
	county_code = '';

	/*because we are using the leading zeros we can just combine the two in one
	fell swoop rather then break up into different data steps*/
	if &count ge 100 then county_code = &count;
	if &count lt 10 then single = &count;

	if &count ge 10 and &count lt 100 then single = &count;

	if county_code = '' then county_code = put(input(single, best3.),z3.);

			length fips $ 5;
			fips = cats(state_code,county_code);

run;

%mend fips;

%fips( hosp_posmerge , FIPS_STATE_CD , FIPS_CNTY_CD)


data hospital_id;
	set hospital_fips;
	drop hospital_num;


	 if MDCL_SCHL_AFLTN_CD=1 then Major_teach= 1; 
			else major_teach=0;
		if CRTFD_BED_CNT >= 300 then large = 1;
			else large = 0;
run;
/* Collect the data from the AHRF*/
data ahrf_data;
	set ahrf.ahrf_2017_2018;
	keep fips poverty black white native asian hispanic homehealthagen hosp_beds_per_cap snf_beds_per_cap pcp_per_cap;
	fips = f00002;
	poverty = F1322315;
	black = F1464010;
	white =  F1463910;
	native = F1465610;
	asian = F1345710;
	hispanic = F0454210;
	pcp = F1467515;
	hospital_beds = F0892115;
	snf_beds = F1321316;
	homehealthagen = F1321416;
	pop_estimate_16 = F1198416;
	pop_estimate_15 = F1198415;
	pop_esimtate_10 = f0453010;

	pcp_per_cap = (pcp/F1198415)*1000;
	hosp_beds_per_cap = (hospital_beds/F1198415)*1000;
	snf_beds_per_cap = (snf_beds/F1198415)*1000;

run; 
/*Prepare the two data sets to merge by the fips code*/
%sort(ahrf_data, fips)
%sort(hospital_id, fips)
 
	data output.final_set;
	merge hospital_id (in = a) ahrf_data (in = b);
	by fips;
	if a;
	if b;
	run;
	/* Descriptive statistis tables requested*/
%sort(output.final_set, Peer_Group_Assignment) 
	proc means data = output.final_set;
	var Dual_Proportion;
	by Peer_Group_Assignment;
	run;

	proc means data = output.final_set;
	var poverty;
	by Peer_Group_Assignment;
	run;
	
	/* Regression anaysis*/
/* THA_TKA HF COPD AMI Pneumonia CABG */


proc logistic  data = output.final_set;
	class fp (ref = '0' )gov (ref = '0' ) major_teach (ref = '0') large (ref = '0');
	model top_ami (event = '1') = fp gov major_teach large poverty black native hispanic pcp_per_cap
	hosp_beds_per_cap snf_beds_per_cap homehealthagen;
	run;






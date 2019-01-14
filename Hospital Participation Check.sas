/*** Supplmental check on racial ethnic health disparities to see if *** 
there are differences in penaltay when number of diagnosis eligible for 
penalty are taken into consideration. Using IPPS, Hospital Compare, and AHRF. 

Program written by Robert Schuldt E-Mail: rschuldt@uams.edu

*****************************************01/14/2018****************************************/

libname output "E:\HRRP\12 19 2018";
libname delta "E:\HRRP";
libname ahrf "\\FileSrv1\CMS_Caregiver\DATA\HRRP\AHRF";
libname delta "E:\HRRP";

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

data ipps_check (rename = (hosp_num = hospital_ccn ))  ;
	set ipps_clean;
	
	
run;
/* Checking to see the number of conditions hospitals have counting to their evaluation*/

data diag_check;
set ipps_check;
	
	array new (6)  f0 f1 f2 f3 f4 f5 ;

	array check (6) check1-check6 ;

		do i = 1 to 6;
			if new(i) ge 24 then check(i) = 1;
				else check(i) = 0;

					end;

			Diag_Check = sum(of check1-check6);
			
			prvdr_num = put(input(hospital_ccn, best12.),z6.);
		run;
/* Bring in file with delta and border designation*/

data delta_border;
	set delta.delta_border;
	where penalty_year = "2019" and (delta_hosp = 1 or border_hosp = 1);
run;

/* Macro for easy sorting */
%macro sort(dataset, sorted);
proc sort data = &dataset;
by &sorted;
run;

%mend sort;

%sort(diag_check, prvdr_num)
%sort(delta_border, prvdr_num)
/* Merge the border hospital file to the IPPS file to identify peer groups*/

data hospital_check;
merge diag_check (in = a) delta_border (in = b);
by prvdr_num;
if a;
if b;
run;
/* Identify which hospitals have a payment penalty based on performance */
data penalty_check;
	set hospital_check;

	if Payment_adjust lt 1 then penalty = 1;
		else penalty = 0;
	run;
proc format;
value pen_y
0 = "Border Penalty"
1 = "Delta Penalty"
;
run;

proc format;
value pen_n
0 = "Border No Penalty"
1 = "Delta No Penalty"
;
run;

proc format;
value del
0 = "Border"
1 = "Delta"
;
run;

%sort(hospital_check, Peer_Group_Assignment)
title 'Average Number of Conditions by Group';
title2 'Delta and Border by Peer Group';

ods escapechar = '^';
goptions reset=all hsize=7in vsize=2in;
ods pdf file='\\FileSrv1\CMS_Caregiver\DATA\Rural Urban Project\Descriptive Stats\Conditions.pdf' 
startpage=no; 

ods pdf text = "^{newline 4}"; 
ods pdf text = "^{style [just=center]}Peer Groups";
	proc means data = penalty_check;
	class delta_hosp;
	var Diag_check;
	by Peer_Group_Assignment;
	format delta_hosp del.;
	
	run;

	proc means data = penalty_check;
	class delta_hosp;
	var Diag_check;
	by Peer_Group_Assignment;
	where penalty = 1;
	format delta_hosp pen_y.;
	run;

	proc means data = penalty_check;
	class delta_hosp;
	var Diag_check;
	by Peer_Group_Assignment;
	where penalty = 0;
	format delta_hosp pen_n.;
	run;

	ods pdf close;

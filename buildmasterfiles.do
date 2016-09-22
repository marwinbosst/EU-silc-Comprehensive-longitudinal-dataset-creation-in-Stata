/*build the freakin masterdataset*/

/*******************************************************/
/*                                                     */
/* SYSTEM REQUIREMENTS: 8gb RAM , 20gb free Discspace  */
/*                         Stata 13 MP, 1h patience    */
/*                                                     */
/*******************************************************/

/*******************************************************/
/*                                                     */
/* This do-file transforms the EU-silc longitudinal    */
/*  UDB files (2013 release) into 4 datasets (D,H,R,P  */ 
/*  -files) containing observations from 2003 to 20013 */
/*                                                     */
/*******************************************************/

/*******************************************************/
/*                                                     */
/* Preparation: Copy the EU-SILC file to your desktop, */
/* if necessary decrypt and unzip ("Unizip here") any  */ 
/* files contained in the folder without changing their*/
/* location. Then update folder location as prompted   */
/* below.                                              */
/*******************************************************/



/* Update file locations */
display in red "******** To update automatically the locations of the files used in the .do file, please right-click the folder 'EU-SILC' go to 'Properties' and copy-paste its location in the command field, without adding any quotation margks or slashes: (for example C:\Users\Paul\Desktop ) " _request(datapath)


/****************************************************************************************************************************
/open the 2013 Household register to get list of rotation groups with maxobs and starting point for masterfile*/
set more off
insheet using "${datapath}\EU-SILC\9 L-2013\UDB_l13D_ver 2013-2 from 01-01-2016.csv", clear
gen year=db010
gen country=db020
gen region=db040
rename db030 hid
rename db075 rotation_group
sort country hid year
/*Block Stata from displaying IDs in exponential format*/
tostring hid, replace
/*count number of times an a household has been interviewd over the years*/
gen obs = 1
bysort country hid: egen totobs=total(obs)
/* get the rotation group(s) that cover most years*/
bysort country: egen maxobs = max(totobs)
gen slctd_rtgrp=0
bysort country: replace slctd_rtgrp=rotation_group if maxobs == totobs
/*need to capture housholds of the selected rotation group that dropped out before planned end of the survey. max() is used to convert the 0 entries in slctd_rtgrp*/
bysort country rotation_group: egen maxrtgrp = max(slctd_rtgrp)
bysort country rotation_group: replace slctd_rtgrp = rotation_group if rotation_group == maxrtgrp
drop maxrtgrp
/* build household and rotationgroup IDs that is unique across releases */
tostring rotation_group, generate(rotation_groupstr)
  bysort country rotation_group: egen maxobsrtgrp = max(totobs)
  egen drpout_year= max(year)
  replace drpout_year = drpout_year + 4 - maxobsrtgrp
tostring drpout_year, replace
gen uhid = country + rotation_groupstr + drpout_year + hid
gen urtgrp = country + rotation_groupstr + drpout_year
drop rotation_groupstr drpout_year maxobsrtgrp
/* checking whether there are duplicates due to errors in unique identifiers. If yes drop them. */
duplicates report year uhid
duplicates drop year uhid, force 
/*this is the masterfile to build the dataset on */
save "${datapath}\EU-SILC\9 L-2013\masterD.dta", replace
/* these are uhid etc. for masterfiles (D,H...)*/
keep slctd_rtgrp urtgrp uhid hid year country totobs maxobs
sort year country hid
save "${datapath}\EU-SILC\9 L-2013\masterIDs.dta", replace
/*these are the selected groups and ID of households for later control */
drop if slctd_rtgrp == 0
gen slctd_urtgrp2013  = urtgrp
gen  slctd_uhid2013 = uhid
gen country2013 = country
sort year country uhid
save "${datapath}\EU-SILC\9 L-2013\2013slctd.dta", replace

/* 
******************************************************************************************************************************
open the 2012 Household register to get data from rotational groups inactive in 2013 and their uhid*/

insheet using "${datapath}\EU-SILC\8 L-2012\UDB_l12D_ver 2012-3 from 01-08-2015.csv", clear
gen year=db010
gen country=db020
gen region=db040
rename db030 hid
rename db075 rotation_group
sort country hid year
/*Block Stata from displaying IDs in exponential format*/
tostring hid, replace
/*count number of times an a household has been interviewd over the years*/
gen obs = 1
bysort country hid: egen totobs=total(obs)
/* get the rotation group(s) that cover most years*/
bysort country: egen maxobs = max(totobs)
gen slctd_rtgrp=0
bysort country: replace slctd_rtgrp=rotation_group if maxobs == totobs
/*need to capture housholds of the selected rotation group that dropped out before planned end of the survey*/
bysort country rotation_group: egen maxrtgrp = max(slctd_rtgrp)
bysort country rotation_group: replace slctd_rtgrp = rotation_group if rotation_group == maxrtgrp
drop maxrtgrp
/* build household and rotation group IDs that is unique across releases */
tostring rotation_group, generate(rotation_groupstr)
  bysort country rotation_group: egen maxobsrtgrp = max(totobs)
  egen drpout_year= max(year)
  replace drpout_year = drpout_year + 4 - maxobsrtgrp
tostring drpout_year, replace
gen uhid = country + rotation_groupstr + drpout_year + hid
gen urtgrp = country + rotation_groupstr + drpout_year
drop rotation_groupstr drpout_year maxobsrtgrp
/* checking whether there are duplicates due to errors in unique identifiers. If yes drop them. */
duplicates report year uhid
duplicates drop year uhid, force 
/* first need to check whether some selected rotationgroups have already been captured in 2013 release and therefore need to be dropped*/
drop if slctd_rtgrp == 0
sort year country uhid
merge 1:1 year country uhid using "${datapath}\EU-SILC\9 L-2013\2013slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
drop if	slctd_urtgrp2013 == urtgrp	
drop _merge	
/*this is the data to be merged to the masterfile*/
save "${datapath}\EU-SILC\8 L-2012\2012D.dta", replace

/*these are the selected groups and IDs of households in 2012 */
keep slctd_rtgrp urtgrp uhid hid year country totobs maxobs
gen slctd_urtgrp2012  = urtgrp
gen  slctd_uhid2012 = uhid
gen country2012 = country
sort year country uhid
save "${datapath}\EU-SILC\8 L-2012\2012slctd.dta", replace

/* 
******************************************************************************************************************************
open the 2011 Household register to get data from rotational groups inactive in previous releases and list of rotation groups with maxobs*/

insheet using "${datapath}\EU-SILC\7 L-2011\UDB_l11D_ver 2011-4 from 01-03-2015.csv", clear
gen year=db010
gen country=db020
gen region=db040
rename db030 hid
rename db075 rotation_group
sort country hid year
/*Block Stata from displaying IDs in exponential format*/
tostring hid, replace
/*count number of times an a household has been interviewd over the years*/
gen obs = 1
bysort country hid: egen totobs=total(obs)
/* get the rotation group(s) that cover most years*/
bysort country: egen maxobs = max(totobs)
gen slctd_rtgrp=0
bysort country: replace slctd_rtgrp=rotation_group if maxobs == totobs
/*need to capture housholds of the selected rotation group that dropped out before planned end of the survey*/
bysort country rotation_group: egen maxrtgrp = max(slctd_rtgrp)
bysort country rotation_group: replace slctd_rtgrp = rotation_group if rotation_group == maxrtgrp
drop maxrtgrp
/* build household and rotation group IDs that is unique across releases */
tostring rotation_group, generate(rotation_groupstr)
  bysort country rotation_group: egen maxobsrtgrp = max(totobs)
  egen drpout_year= max(year)
  replace drpout_year = drpout_year + 4 - maxobsrtgrp
tostring drpout_year, replace
gen uhid = country + rotation_groupstr + drpout_year + hid
gen urtgrp = country + rotation_groupstr + drpout_year
drop rotation_groupstr drpout_year maxobsrtgrp
/* checking whether there are duplicates due to errors in unique identifiers. If yes drop them. */
duplicates report year uhid
duplicates drop year uhid, force 
/* first need to check whether some selected rotationgroups have already been captured in 2013 or 2012 release and therefore need to be dropped*/
drop if slctd_rtgrp == 0
sort year country uhid
merge 1:1 year country uhid using "${datapath}\EU-SILC\8 L-2012\2012slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2012 == urtgrp
drop if	slctd_urtgrp2012 == urtgrp
drop _merge
sort year country uhid
merge 1:1 year country uhid using "${datapath}\EU-SILC\9 L-2013\2013slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2013 == urtgrp
drop if	slctd_urtgrp2013 == urtgrp
drop _merge
/*this is the data to be merged to the masterfile*/
save "${datapath}\EU-SILC\7 L-2011\2011D.dta", replace

/*these are the selected groups and IDs of households in 2012 */
keep slctd_rtgrp urtgrp uhid hid year country totobs maxobs
drop if slctd_rtgrp == 0
gen slctd_urtgrp2011  = urtgrp
gen  slctd_uhid2011 = uhid
gen country2011 = country
sort year country uhid
save "${datapath}\EU-SILC\7 L-2011\2011slctd.dta", replace

/* 
******************************************************************************************************************************
open the 2010 Household register to get data from rotational groups inactive in previous releases and list of rotation groups with maxobs*/

insheet using "${datapath}\EU-SILC\6 L-2010\UDB_l10D_ver 2010-5 from 01-08-2014.csv", clear
gen year=db010
gen country=db020
gen region=db040
rename db030 hid
rename db075 rotation_group
sort country hid year
/*Block Stata from displaying IDs in exponential format*/
tostring hid, replace
/*count number of times an a household has been interviewd over the years*/
gen obs = 1
bysort country hid: egen totobs=total(obs)
/* get the rotation group(s) that cover most years*/
bysort country: egen maxobs = max(totobs)
gen slctd_rtgrp=0
bysort country: replace slctd_rtgrp=rotation_group if maxobs == totobs
/*need to capture housholds of the selected rotation group that dropped out before planned end of the survey*/
bysort country rotation_group: egen maxrtgrp = max(slctd_rtgrp)
bysort country rotation_group: replace slctd_rtgrp = rotation_group if rotation_group == maxrtgrp
drop maxrtgrp
/* build household and rotation group IDs that is unique across releases */
tostring rotation_group, generate(rotation_groupstr)
  bysort country rotation_group: egen maxobsrtgrp = max(totobs)
  egen drpout_year= max(year)
  replace drpout_year = drpout_year + 4 - maxobsrtgrp
tostring drpout_year, replace
gen uhid = country + rotation_groupstr + drpout_year + hid
gen urtgrp = country + rotation_groupstr + drpout_year
drop rotation_groupstr drpout_year maxobsrtgrp
/* checking whether there are duplicates due to errors in unique identifiers. If yes drop them. */
duplicates report year uhid
duplicates drop year uhid, force 
/* first need to check whether some selected rotationgroups have already been captured in 2013, 2012 or 2011 release and therefore need to be dropped*/
drop if slctd_rtgrp == 0
sort year country uhid
merge 1:1 year country uhid using "${datapath}\EU-SILC\7 L-2011\2011slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2011 == urtgrp
drop if	slctd_urtgrp2011 == urtgrp
drop _merge
merge 1:1 year country uhid using "${datapath}\EU-SILC\8 L-2012\2012slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2012 == urtgrp
drop if	slctd_urtgrp2012 == urtgrp
drop _merge
sort year country uhid
merge 1:1 year country uhid using "${datapath}\EU-SILC\9 L-2013\2013slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2013 == urtgrp
drop if	slctd_urtgrp2013 == urtgrp
drop _merge
/*this is the data to be merged to the masterfile*/
save "${datapath}\EU-SILC\6 L-2010\2010D.dta", replace
/*these are the selected groups and IDs of households in 2010 */
keep slctd_rtgrp urtgrp uhid hid year country totobs maxobs
drop if slctd_rtgrp == 0
gen slctd_urtgrp2010  = urtgrp
gen  slctd_uhid2010 = uhid
gen country2010 = country
sort year country uhid
save "${datapath}\EU-SILC\6 L-2010\2010slctd.dta", replace

/* 
******************************************************************************************************************************
open the 2009 Household register to get data from rotational groups inactive in previous releases and list of rotation groups with maxobs*/

insheet using "${datapath}\EU-SILC\5 L-2009\UDB_l09D_ver 2009-4 from 01-03-2013.csv", clear
gen year=db010
gen country=db020
gen region=db040
rename db030 hid
rename db075 rotation_group
sort country hid year
/*Block Stata from displaying IDs in exponential format*/
tostring hid, replace
/*count number of times an a household has been interviewd over the years*/
gen obs = 1
bysort country hid: egen totobs=total(obs)
/* get the rotation group(s) that cover most years*/
bysort country: egen maxobs = max(totobs)
gen slctd_rtgrp=0
bysort country: replace slctd_rtgrp=rotation_group if maxobs == totobs
/*need to capture housholds of the selected rotation group that dropped out before planned end of the survey*/
bysort country rotation_group: egen maxrtgrp = max(slctd_rtgrp)
bysort country rotation_group: replace slctd_rtgrp = rotation_group if rotation_group == maxrtgrp
drop maxrtgrp
/* build household and rotation group IDs that is unique across releases */
tostring rotation_group, generate(rotation_groupstr)
  bysort country rotation_group: egen maxobsrtgrp = max(totobs)
  egen drpout_year= max(year)
  replace drpout_year = drpout_year + 4 - maxobsrtgrp
tostring drpout_year, replace
gen uhid = country + rotation_groupstr + drpout_year + hid
gen urtgrp = country + rotation_groupstr + drpout_year
drop rotation_groupstr drpout_year maxobsrtgrp
/* checking whether there are duplicates due to errors in unique identifiers. If yes drop them. */
duplicates report year uhid
duplicates drop year uhid, force 
/* first need to check whether some selected rotationgroups have already been captured in 2012, 2011 or 2010 release and therefore need to be dropped*/
drop if slctd_rtgrp == 0
sort year country uhid
merge 1:1 year country uhid using "${datapath}\EU-SILC\6 L-2010\2010slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2010 == urtgrp
drop if	slctd_urtgrp2010 == urtgrp
drop _merge
merge 1:1 year country uhid using "${datapath}\EU-SILC\7 L-2011\2011slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2011 == urtgrp
drop if	slctd_urtgrp2011 == urtgrp
drop _merge
merge 1:1 year country uhid using "${datapath}\EU-SILC\8 L-2012\2012slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2012 == urtgrp
drop if	slctd_urtgrp2012 == urtgrp
drop _merge
sort year country uhid
/*this is the data to be merged to the masterfile*/
save "${datapath}\EU-SILC\5 L-2009\2009D.dta", replace
/*these are the selected groups and IDs of households in 2010 */
keep slctd_rtgrp urtgrp uhid hid year country totobs maxobs
drop if slctd_rtgrp == 0
gen slctd_urtgrp2009  = urtgrp
gen  slctd_uhid2009 = uhid
gen country2009 = country
sort year country uhid
save "${datapath}\EU-SILC\5 L-2009\2009slctd.dta", replace

/* 
******************************************************************************************************************************
open the 2008 Household register to get data from rotational groups inactive in previous releases and list of rotation groups with maxobs*/

insheet using "${datapath}\EU-SILC\4 L-2008\UDB_l08D_ver 2008-4 from 01-03-2012.csv", clear
gen year=db010
gen country=db020
replace country = "EL" if country == "GR"
gen region=db040
rename db030 hid
rename db075 rotation_group
sort country hid year
/*Block Stata from displaying IDs in exponential format*/
tostring hid , replace
/*count number of times an a household has been interviewd over the years*/
gen obs = 1
bysort country hid: egen totobs=total(obs)
/* get the rotation group(s) that cover most years*/
bysort country: egen maxobs = max(totobs)
gen slctd_rtgrp=0
bysort country: replace slctd_rtgrp=rotation_group if maxobs == totobs
/*need to capture housholds of the selected rotation group that dropped out before planned end of the survey*/
bysort country rotation_group: egen maxrtgrp = max(slctd_rtgrp)
bysort country rotation_group: replace slctd_rtgrp = rotation_group if rotation_group == maxrtgrp
drop maxrtgrp
/* build household and rotation group IDs that is unique across releases */
tostring rotation_group, generate(rotation_groupstr)
  bysort country rotation_group: egen maxobsrtgrp = max(totobs)
  egen drpout_year= max(year)
  replace drpout_year = drpout_year + 4 - maxobsrtgrp
tostring drpout_year, replace
gen uhid = country + rotation_groupstr + drpout_year + hid
gen urtgrp = country + rotation_groupstr + drpout_year
drop rotation_groupstr drpout_year maxobsrtgrp
/* checking whether there are duplicates due to errors in unique identifiers. If yes drop them. */
duplicates report year uhid
duplicates drop year uhid, force 
/* first need to check whether some selected rotationgroups have already been captured in 2011, 2010 or 2009 release and therefore need to be dropped*/
drop if slctd_rtgrp == 0
sort year country uhid
merge 1:1 year country uhid using "${datapath}\EU-SILC\5 L-2009\2009slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2009 == urtgrp
drop if	slctd_urtgrp2009 == urtgrp
drop _merge
merge 1:1 year country uhid using "${datapath}\EU-SILC\6 L-2010\2010slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2010 == urtgrp
drop if	slctd_urtgrp2010 == urtgrp
drop _merge
merge 1:1 year country uhid using "${datapath}\EU-SILC\7 L-2011\2011slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2011 == urtgrp
drop if	slctd_urtgrp2011 == urtgrp
drop _merge
sort year country uhid
/*this is the data to be merged to the masterfile*/
save "${datapath}\EU-SILC\4 L-2008\2008D.dta", replace
/*these are the selected groups and IDs of households in 2010 */
keep slctd_rtgrp urtgrp uhid hid year country totobs maxobs
drop if slctd_rtgrp == 0
gen slctd_urtgrp2008  = urtgrp
gen  slctd_uhid2008 = uhid
gen country2008 = country
sort year country uhid
save "${datapath}\EU-SILC\4 L-2008\2008slctd.dta", replace

/* 
******************************************************************************************************************************
open the 2007 Household register to get data from rotational groups inactive in previous releases and list of rotation groups with maxobs*/

insheet using "${datapath}\EU-SILC\3 L-2007\UDB_l07D_ver 2007-5 from 01-08-2011.csv", clear
gen year=db010
gen country=db020
replace country = "EL" if country == "GR"
gen region=db040
rename db030 hid
rename db075 rotation_group
sort country hid year
/*Block Stata from displaying IDs in exponential format*/
tostring hid, replace
/*count number of times an a household has been interviewd over the years*/
gen obs = 1
bysort country hid: egen totobs=total(obs)
/* get the rotation group(s) that cover most years*/
bysort country: egen maxobs = max(totobs)
gen slctd_rtgrp=0
bysort country: replace slctd_rtgrp=rotation_group if maxobs == totobs
/*need to capture housholds of the selected rotation group that dropped out before planned end of the survey*/
bysort country rotation_group: egen maxrtgrp = max(slctd_rtgrp)
bysort country rotation_group: replace slctd_rtgrp = rotation_group if rotation_group == maxrtgrp
drop maxrtgrp
/* build household and rotation group IDs that is unique across releases */
tostring rotation_group, generate(rotation_groupstr)
  bysort country rotation_group: egen maxobsrtgrp = max(totobs)
  egen drpout_year= max(year)
  replace drpout_year = drpout_year + 4 - maxobsrtgrp
tostring drpout_year, replace
gen uhid = country + rotation_groupstr + drpout_year + hid
gen urtgrp = country + rotation_groupstr + drpout_year
drop rotation_groupstr drpout_year maxobsrtgrp
/* checking whether there are duplicates due to errors in unique identifiers. If yes drop them. */
duplicates report year uhid
duplicates drop year uhid, force 
/* first need to check whether some selected rotationgroups have already been captured in 2010, 2009 or 2008 release and therefore need to be dropped*/
drop if  slctd_rtgrp == 0
sort year country uhid
merge 1:1 year country uhid using "${datapath}\EU-SILC\4 L-2008\2008slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2008 == urtgrp
drop if	slctd_urtgrp2008 == urtgrp
drop _merge
merge 1:1 year country uhid using "${datapath}\EU-SILC\5 L-2009\2009slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2009 == urtgrp
drop if	slctd_urtgrp2009 == urtgrp
drop _merge
merge 1:1 year country uhid using "${datapath}\EU-SILC\6 L-2010\2010slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2010 == urtgrp
drop if	 slctd_urtgrp2010 == urtgrp
drop _merge
sort year country uhid
/*this is the data to be merged to the masterfile*/
save "${datapath}\EU-SILC\3 L-2007\2007D.dta", replace
/*these are the selected groups and IDs of households in 2010 */
keep slctd_rtgrp urtgrp uhid hid year country totobs maxobs
drop if  slctd_rtgrp == 0
gen slctd_urtgrp2007  = urtgrp
gen  slctd_uhid2007 = uhid
gen country2007 = country
sort year country uhid
save "${datapath}\EU-SILC\3 L-2007\2007slctd.dta", replace

/* 
******************************************************************************************************************************
open the 2006 Household register to get data from rotational groups inactive in previous releases and list of rotation groups with maxobs*/

insheet using "${datapath}\EU-SILC\2 L-2006\UDB_L06D_ver 2006-2 from 01-03-2009.csv", clear
gen year=db010
gen country=db020
replace country = "EL" if country == "GR"
gen region=db040
rename db030 hid
rename db075 rotation_group
sort country hid year
/*Block Stata from displaying IDs in exponential format*/
tostring hid, replace
/*count number of times an a household has been interviewd over the years*/
gen obs = 1
bysort country hid: egen totobs=total(obs)
/* get the rotation group(s) that cover most years*/
bysort country: egen maxobs = max(totobs)
gen slctd_rtgrp=0
bysort country: replace slctd_rtgrp=rotation_group if maxobs == totobs
/*need to capture housholds of the selected rotation group that dropped out before planned end of the survey*/
bysort country rotation_group: egen maxrtgrp = max(slctd_rtgrp)
bysort country rotation_group: replace slctd_rtgrp = rotation_group if rotation_group == maxrtgrp
drop maxrtgrp
/* build household and rotation group IDs that is unique across releases */
tostring rotation_group, generate(rotation_groupstr)
  bysort country rotation_group: egen maxobsrtgrp = max(totobs)
  egen drpout_year= max(year)
  replace drpout_year = drpout_year + 4 - maxobsrtgrp
tostring drpout_year, replace
gen uhid = country + rotation_groupstr + drpout_year + hid
gen urtgrp = country + rotation_groupstr + drpout_year
drop rotation_groupstr drpout_year maxobsrtgrp
/* checking whether there are duplicates due to errors in unique identifiers. If yes drop them. */
duplicates report year uhid
duplicates drop year uhid, force 
/* first need to check whether some selected rotationgroups have already been captured in 2009, 2008 or 2007 release and therefore need to be dropped*/
drop if slctd_rtgrp == 0
sort year country uhid
merge 1:1 year country uhid using "${datapath}\EU-SILC\3 L-2007\2007slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2007 == urtgrp
drop if	slctd_urtgrp2007 == urtgrp
drop _merge
merge 1:1 year country uhid using "${datapath}\EU-SILC\4 L-2008\2008slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2008 == urtgrp
drop if	slctd_urtgrp2008 == urtgrp
drop _merge
merge 1:1 year country uhid using "${datapath}\EU-SILC\5 L-2009\2009slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2009 == urtgrp
drop if	slctd_urtgrp2009 == urtgrp
drop _merge
sort year country uhid
/*this is the data to be merged to the masterfile*/
save "${datapath}\EU-SILC\2 L-2006\2006D.dta", replace
/*these are the selected groups and IDs of households in 2010 */
keep slctd_rtgrp urtgrp uhid hid year country totobs maxobs
drop if slctd_rtgrp == 0
gen slctd_urtgrp2006  = urtgrp
gen  slctd_uhid2006 = uhid
gen country2006 = country
sort year country uhid
save "${datapath}\EU-SILC\2 L-2006\2006slctd.dta", replace

/* 
******************************************************************************************************************************
open the 2005 Household register to get data from rotational groups inactive in previous releases and list of rotation groups with maxobs*/

insheet using "${datapath}\EU-SILC\1 L-2005\UDB_L05D_ver 2005-1 from 15-09-07.csv", clear
gen year=db010
gen country=db020
replace country = "EL" if country == "GR"
gen region=db040
rename db030 hid
rename db075 rotation_group
sort country hid year
/*Block Stata from displaying IDs in exponential format*/
tostring hid , replace
/*count number of times an a household has been interviewd over the years*/
gen obs = 1
bysort country hid: egen totobs=total(obs)
/* get the rotation group(s) that cover most years*/
bysort country: egen maxobs = max(totobs)
gen slctd_rtgrp=0
bysort country: replace slctd_rtgrp=rotation_group if maxobs == totobs
/*need to capture housholds of the selected rotation group that dropped out before planned end of the survey*/
bysort country rotation_group: egen maxrtgrp = max(slctd_rtgrp)
bysort country rotation_group: replace slctd_rtgrp = rotation_group if rotation_group == maxrtgrp
drop maxrtgrp
/* build household and rotation group IDs that is unique across releases */
tostring rotation_group, generate(rotation_groupstr)
  bysort country rotation_group: egen maxobsrtgrp = max(totobs)
  egen drpout_year= max(year)
  replace drpout_year = drpout_year + 4 - maxobsrtgrp
tostring drpout_year, replace
gen uhid = country + rotation_groupstr + drpout_year + hid
gen urtgrp = country + rotation_groupstr + drpout_year
drop rotation_groupstr drpout_year maxobsrtgrp
/* checking whether there are duplicates due to errors in unique identifiers. If yes drop them. */
duplicates report year uhid
duplicates drop year uhid, force 
/* first need to check whether some selected rotationgroups have already been captured in 2008, 2007 or 2006 release and therefore need to be dropped*/
drop if slctd_rtgrp == 0
sort year country uhid
merge 1:1 year country uhid using "${datapath}\EU-SILC\2 L-2006\2006slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2006 == urtgrp
drop if	slctd_urtgrp2006 == urtgrp
drop _merge
merge 1:1 year country uhid using "${datapath}\EU-SILC\3 L-2007\2007slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2007 == urtgrp
drop if	slctd_urtgrp2007 == urtgrp
drop _merge
merge 1:1 year country uhid using "${datapath}\EU-SILC\4 L-2008\2008slctd.dta"
/* _merge==3 indicates that a selected rotational group has already been selected in the previous release */
			/*check wheter there are issues, i.e. which obs/groups are dropped*/
			tab country year if _merge == 3
			tab country year if slctd_urtgrp2008 == urtgrp
drop if	slctd_urtgrp2008 == urtgrp
drop _merge
sort year country uhid
/*this is the data to be merged to the masterfile*/
save "${datapath}\EU-SILC\1 L-2005\2005D.dta", replace
/*these are the selected groups and IDs of households in 2010 */
keep slctd_rtgrp urtgrp uhid hid year country totobs maxobs
drop if slctd_rtgrp == 0
gen slctd_urtgrp2006  = urtgrp
gen  slctd_uhid2006 = uhid
gen country2006 = country
sort year country uhid
save "${datapath}\EU-SILC\1 L-2005\2005slctd.dta", replace

/* now create one D file containing data from all releases (we use merge instead of append to check for inconsistencies, _merge==3 >> trouble */
use "${datapath}\EU-SILC\9 L-2013\masterD.dta", clear
merge 1:1 year uhid using "${datapath}\EU-SILC\8 L-2012\2012D.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\7 L-2011\2011D.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\6 L-2010\2010D.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\5 L-2009\2009D.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\4 L-2008\2008D.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\3 L-2007\2007D.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\2 L-2006\2006D.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\1 L-2005\2005D.dta"
drop _merge
save "${datapath}\EU-SILC\9 L-2013\masterD.dta", replace

/* create one H file containing data from all releases */
/* first step: for each release merge slctd file with H file, and keep obs only if there is match _merge==3. Notice also non-responses (present in D but not in H) are dropped but 
can be readded later by merging masterD with masterH*/
insheet using "${datapath}\EU-SILC\9 L-2013\UDB_l13H_ver 2013-2 from 01-01-2016.csv", clear
gen year = hb010
gen hid = hb030 
tostring hid, replace
gen country = hb020 
sort year country hid 
/*checking for duplicates or errors in hid*/
duplicates report year country hid
duplicates drop year country hid, force
merge 1:1 year country hid using "${datapath}\EU-SILC\9 L-2013\masterIDs.dta"
keep if _merge==3
drop _merge
save "${datapath}\EU-SILC\9 L-2013\masterH.dta", replace

/*matching H files from each release with the 20XXslctd files*/

insheet using "${datapath}\EU-SILC\8 L-2012\UDB_l12H_ver 2012-3 from 01-08-2015.csv", clear
gen year = hb010
gen hid = hb030 
tostring hid, replace
gen country = hb020 
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country hid
duplicates drop year country hid, force
merge 1:1 year country hid using "${datapath}\EU-SILC\8 L-2012\2012slctd.dta"
keep if _merge==3
drop _merge
save "${datapath}\EU-SILC\8 L-2012\2012H.dta", replace

insheet using "${datapath}\EU-SILC\7 L-2011\UDB_l11H_ver 2011-4 from 01-03-2015.csv", clear
gen year = hb010
gen hid = hb030 
tostring hid, replace
gen country = hb020 
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country hid
duplicates drop year country hid, force
merge 1:1 year country hid using "${datapath}\EU-SILC\7 L-2011\2011slctd.dta"
keep if _merge==3
drop _merge
destring hb100 , replace force
save "${datapath}\EU-SILC\7 L-2011\2011H.dta", replace

insheet using "${datapath}\EU-SILC\6 L-2010\UDB_l10H_ver 2010-5 from 01-08-2014.csv", clear
gen year = hb010
gen hid = hb030 
tostring hid, replace
gen country = hb020 
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country hid
duplicates drop year country hid, force
merge 1:1 year country hid using "${datapath}\EU-SILC\6 L-2010\2010slctd.dta"
keep if _merge==3
drop _merge
save "${datapath}\EU-SILC\6 L-2010\2010H.dta", replace

insheet using "${datapath}\EU-SILC\5 L-2009\UDB_l09H_ver 2009-4 from 01-03-2013.csv", clear
gen year = hb010
gen hid = hb030 
tostring hid, replace
gen country = hb020 
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country hid
duplicates drop year country hid, force
merge 1:1 year country hid using "${datapath}\EU-SILC\5 L-2009\2009slctd.dta"
keep if _merge==3
drop _merge
save "${datapath}\EU-SILC\5 L-2009\2009H.dta", replace

insheet using "${datapath}\EU-SILC\4 L-2008\UDB_l08H_ver 2008-4 from 01-03-2012.csv", clear
gen year = hb010
gen hid = hb030 
tostring hid, replace
gen country = hb020 
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country hid
duplicates drop year country hid, force
merge 1:1 year country hid using "${datapath}\EU-SILC\4 L-2008\2008slctd.dta"
keep if _merge==3
drop _merge
save "${datapath}\EU-SILC\4 L-2008\2008H.dta", replace

insheet using "${datapath}\EU-SILC\3 L-2007\UDB_l07H_ver 2007-5 from 01-08-2011.csv", clear
gen year = hb010
gen hid = hb030 
tostring hid, replace
gen country = hb020 
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country hid
duplicates drop year country hid, force
merge 1:1 year country hid using "${datapath}\EU-SILC\3 L-2007\2007slctd.dta"
keep if _merge==3
drop _merge
save "${datapath}\EU-SILC\3 L-2007\2007H.dta", replace

insheet using "${datapath}\EU-SILC\2 L-2006\UDB_L06H_ver 2006-2 from 01-03-2009.csv", clear
gen year = hb010
gen hid = hb030 
tostring hid, replace
gen country = hb020 
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country hid
duplicates drop year country hid, force
merge 1:1 year country hid using "${datapath}\EU-SILC\2 L-2006\2006slctd.dta"
keep if _merge==3
drop _merge
save "${datapath}\EU-SILC\2 L-2006\2006H.dta", replace

insheet using "${datapath}\EU-SILC\1 L-2005\UDB_L05H_ver 2005-1 from 15-09-07.csv", clear
gen year = hb010
gen hid = hb030 
tostring hid, replace
gen country = hb020 
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country hid
duplicates drop year country hid, force
merge 1:1 year country hid using "${datapath}\EU-SILC\1 L-2005\2005slctd.dta"
keep if _merge==3
drop _merge
save "${datapath}\EU-SILC\1 L-2005\2005H.dta", replace

/* second step, merge masterH with the 20XXH files from previous releases.*/

use "${datapath}\EU-SILC\9 L-2013\masterH.dta", clear
merge 1:1 year uhid using "${datapath}\EU-SILC\8 L-2012\2012H.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\7 L-2011\2011H.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\6 L-2010\2010H.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\5 L-2009\2009H.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\4 L-2008\2008H.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\3 L-2007\2007H.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\2 L-2006\2006H.dta"
drop _merge
merge 1:1 year uhid using "${datapath}\EU-SILC\1 L-2005\2005H.dta"
drop _merge
/* This is all household data (H file) 2003 - 20013 */
save "${datapath}\EU-SILC\9 L-2013\masterH.dta", replace


/********************************************************************************************************/
/********************************************************************************************************/
/*build masterfile for personal register files (R files)*/
/* first step: for each release merge slctd file with R file, and keep obs only if there is match _merge==3. */
insheet using "${datapath}\EU-SILC\9 L-2013\UDB_l13R_ver 2013-2 from 01-01-2016.csv", clear
tostring rb030, replace
tostring rb040, replace 
gen year = rb010
gen hid = rb040
gen pid = rb030 
gen country = rb020
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (D file) and drop households that are only in the D file, but not in the R file */
merge m:1 year country hid using "${datapath}\EU-SILC\9 L-2013\masterIDs.dta"
keep if _merge == 3 
drop _merge 
/*genereate personal IDs that are unique across all releases by taking the last to numbers of pid and adding them to uhid*/
gen suhid = substr(uhid, 1, 7)
 gen upid = suhid + pid 
 drop suhid
/*this is the masterfileR where to add data from previous releases to */
save "${datapath}\EU-SILC\9 L-2013\masterR", replace
keep hid pid country year upid uhid 
/*these are IDs that will be used to build masterP */
save "${datapath}\EU-SILC\9 L-2013\masterIDsR", replace


/*merge indivdual registers with 20XXslcd files to select the individuals contained in the rotational groups from previous releases we are interested in */
/*merging 2012 data*/
insheet using "${datapath}\EU-SILC\8 L-2012\UDB_l12R_ver 2012-3 from 01-08-2015.csv", clear
tostring rb030, replace
tostring rb040, replace 
gen year = rb010
gen hid = rb040
gen pid = rb030 
gen country = rb020
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (D file) and drop households that are only in the D file, but not in the R file */
merge m:1 year country hid using "${datapath}\EU-SILC\8 L-2012\2012slctd.dta"
keep if _merge == 3 
drop _merge 
/*genereate personal IDs that are unique across all releases by taking the last to numbers of pid and adding them to uhid*/
gen suhid = substr(uhid, 1, 7)
gen upid = suhid + pid 
drop suhid
save "${datapath}\EU-SILC\8 L-2012\2012R.dta", replace
keep hid pid country year upid uhid 
/*these are IDs that will be used to build masterP */
save "${datapath}\EU-SILC\8 L-2012\2012slctdR.dta", replace

/*merging 2011 data*/
insheet using "${datapath}\EU-SILC\7 L-2011\UDB_l11R_ver 2011-4 from 01-03-2015.csv", clear
tostring rb030, replace
tostring rb040, replace 
gen year = rb010
gen hid = rb040
gen pid = rb030 
gen country = rb020
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (D file) and drop households that are only in the D file, but not in the R file */
merge m:1 year country hid using "${datapath}\EU-SILC\7 L-2011\2011slctd.dta"
keep if _merge == 3 
drop _merge 
/*genereate personal IDs that are unique across all releases by taking the last to numbers of pid and adding them to uhid*/
gen suhid = substr(uhid, 1, 7)
gen upid = suhid + pid 
drop suhid
save "${datapath}\EU-SILC\7 L-2011\2011R.dta", replace
keep hid pid country year upid uhid 
/*these are IDs that will be used to build masterP */
save "${datapath}\EU-SILC\7 L-2011\2011slctdR.dta", replace

/*merging 2010 data*/
insheet using "${datapath}\EU-SILC\6 L-2010\UDB_l10R_ver 2010-5 from 01-08-2014.csv", clear
tostring rb030, replace
tostring rb040, replace 
gen year = rb010
gen hid = rb040
gen pid = rb030 
gen country = rb020
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (D file) and drop households that are only in the D file, but not in the R file */
merge m:1 year country hid using "${datapath}\EU-SILC\6 L-2010\2010slctd.dta"
keep if _merge == 3 
drop _merge 
/*genereate personal IDs that are unique across all releases by taking the last to numbers of pid and adding them to uhid*/
gen suhid = substr(uhid, 1, 7)
gen upid = suhid + pid 
drop suhid
save "${datapath}\EU-SILC\6 L-2010\2010R.dta", replace
keep hid pid country year upid uhid 
/*these are IDs that will be used to build masterP */
save "${datapath}\EU-SILC\6 L-2010\2010slctdR.dta", replace

/*merging 2009 data*/
insheet using "${datapath}\EU-SILC\5 L-2009\UDB_l09R_ver 2009-4 from 01-03-2013.csv", clear
tostring rb030, replace
tostring rb040, replace 
gen year = rb010
gen hid = rb040
gen pid = rb030 
gen country = rb020
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (D file) and drop households that are only in the D file, but not in the R file */
merge m:1 year country hid using "${datapath}\EU-SILC\5 L-2009\2009slctd.dta"
keep if _merge == 3 
drop _merge 
/*genereate personal IDs that are unique across all releases by taking the last to numbers of pid and adding them to uhid*/
gen suhid = substr(uhid, 1, 7)
gen upid = suhid + pid 
drop suhid
save "${datapath}\EU-SILC\5 L-2009\2009R.dta", replace
keep hid pid country year upid uhid 
/*these are IDs that will be used to build masterP */
save "${datapath}\EU-SILC\5 L-2009\2009slctdR.dta", replace

/*merging 2008 data*/
insheet using "${datapath}\EU-SILC\4 L-2008\UDB_l08R_ver 2008-4 from 01-03-2012.csv", clear
tostring rb030, replace
tostring rb040, replace 
gen year = rb010
gen hid = rb040
gen pid = rb030 
gen country = rb020
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (D file) and drop households that are only in the D file, but not in the R file */
merge m:1 year country hid using "${datapath}\EU-SILC\4 L-2008\2008slctd.dta"
keep if _merge == 3 
drop _merge 
/*genereate personal IDs that are unique across all releases by taking the last to numbers of pid and adding them to uhid*/
gen suhid = substr(uhid, 1, 7)
gen upid = suhid + pid 
drop suhid
save "${datapath}\EU-SILC\4 L-2008\2008R.dta", replace
keep hid pid country year upid uhid 
/*these are IDs that will be used to build masterP */
save "${datapath}\EU-SILC\4 L-2008\2008slctdR.dta", replace

/*merging 2007 data*/
insheet using "${datapath}\EU-SILC\3 L-2007\UDB_l07R_ver 2007-5 from 01-08-2011.csv", clear
tostring rb030, replace
tostring rb040, replace 
gen year = rb010
gen hid = rb040
gen pid = rb030 
gen country = rb020
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (D file) and drop households that are only in the D file, but not in the R file */
merge m:1 year country hid using "${datapath}\EU-SILC\3 L-2007\2007slctd.dta"
keep if _merge == 3 
drop _merge 
/*genereate personal IDs that are unique across all releases by taking the last to numbers of pid and adding them to uhid*/
gen suhid = substr(uhid, 1, 7)
gen upid = suhid + pid 
drop suhid
save "${datapath}\EU-SILC\3 L-2007\2007R.dta", replace
keep hid pid country year upid uhid 
/*these are IDs that will be used to build masterP */
save "${datapath}\EU-SILC\3 L-2007\2007slctdR.dta", replace

*merging 2006 data*/
insheet using "${datapath}\EU-SILC\2 L-2006\UDB_L06R_ver 2006-2 from 01-03-2009.csv", clear
tostring rb030, replace
tostring rb040, replace 
gen year = rb010
gen hid = rb040
gen pid = rb030 
gen country = rb020
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (D file) and drop households that are only in the D file, but not in the R file */
merge m:1 year country hid using "${datapath}\EU-SILC\2 L-2006\2006slctd.dta"
keep if _merge == 3 
drop _merge 
/*genereate personal IDs that are unique across all releases by taking the last to numbers of pid and adding them to uhid*/
gen suhid = substr(uhid, 1, 7)
gen upid = suhid + pid 
drop suhid
save "${datapath}\EU-SILC\2 L-2006\2006R.dta", replace
keep hid pid country year upid uhid 
/*these are IDs that will be used to build masterP */
save "${datapath}\EU-SILC\2 L-2006\2006slctdR.dta", replace

/*merging 2005 data*/
insheet using "${datapath}\EU-SILC\1 L-2005\UDB_L05R_ver 2005-1 from 15-09-07.csv", clear
tostring rb030, replace
tostring rb040, replace 
gen year = rb010
gen hid = rb040
gen pid = rb030 
gen country = rb020
sort year country hid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (D file) and drop households that are only in the D file, but not in the R file */
merge m:1 year country hid using "${datapath}\EU-SILC\1 L-2005\2005slctd.dta"
keep if _merge == 3 
drop _merge 
/*genereate personal IDs that are unique across all releases by taking the last to numbers of pid and adding them to uhid*/
gen suhid = substr(uhid, 1, 7)
gen upid = suhid + pid 
drop suhid
save "${datapath}\EU-SILC\1 L-2005\2005R.dta", replace
keep hid pid country year upid uhid 
/*these are IDs that will be used to build masterP */
save "${datapath}\EU-SILC\1 L-2005\2005slctdR.dta", replace

/*merge data from all releases with masterfile. 
!! this process is memory intensive. */
use "${datapath}\EU-SILC\9 L-2013\masterR.dta", clear
merge 1:1 year upid using "${datapath}\EU-SILC\8 L-2012\2012R.dta"
drop _merge
merge 1:1 year upid using "${datapath}\EU-SILC\7 L-2011\2011R.dta"
drop _merge
merge 1:1 year upid using "${datapath}\EU-SILC\6 L-2010\2010R.dta"
drop _merge
merge 1:1 year upid using "${datapath}\EU-SILC\5 L-2009\2009R.dta"
drop _merge
save "${datapath}\EU-SILC\9 L-2013\masterR.dta", replace
clear
use "${datapath}\EU-SILC\9 L-2013\masterR.dta"
merge 1:1 year upid using "${datapath}\EU-SILC\4 L-2008\2008R.dta"
drop _merge
merge 1:1 year upid using "${datapath}\EU-SILC\3 L-2007\2007R.dta"
drop _merge
save "${datapath}\EU-SILC\9 L-2013\masterR.dta", replace
clear
use "${datapath}\EU-SILC\9 L-2013\masterR.dta"
merge 1:1 year upid using "${datapath}\EU-SILC\2 L-2006\2006R.dta"
drop _merge
save "${datapath}\EU-SILC\9 L-2013\masterR.dta", replace
clear
use "${datapath}\EU-SILC\9 L-2013\masterR.dta"
merge 1:1 year upid using "${datapath}\EU-SILC\1 L-2005\2005R.dta"
drop _merge
/* This is the personal register file 2003 - 20013 */
save "${datapath}\EU-SILC\9 L-2013\masterR.dta", replace

/********************************************************************************************************/
/********************************************************************************************************/
/*build masterfile for personal data files (P files)*/
/* first step: for each release merge slctdR file with P file, and keep obs only if there is match _merge==3. */
insheet using "${datapath}\EU-SILC\9 L-2013\UDB_l13P_ver 2013-2 from 01-01-2016.csv", clear
tostring pb030, replace
rename pb010 year
rename pb030 pid   
rename pb020 country
sort year country pid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (P file) and drop households that are only in the D file, but not in the R file */
merge 1:1 year country pid using "${datapath}\EU-SILC\9 L-2013\masterIDsR.dta"
keep if _merge == 3 
drop _merge 
/*this is the masterfileR where to add data from previous releases to */
save "${datapath}\EU-SILC\9 L-2013\masterP", replace

/*selecting 2012 release data*/
insheet using "${datapath}\EU-SILC\8 L-2012\UDB_l12P_ver 2012-3 from 01-08-2015.csv", clear
tostring pb030, replace
rename pb010 year
rename pb030 pid   
rename pb020 country
sort year country pid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (P file) and drop households that are only in the D file, but not in the R file */
merge 1:1 year country pid using "${datapath}\EU-SILC\8 L-2012\2012slctdR.dta"
keep if _merge == 3 
drop _merge 
save "${datapath}\EU-SILC\8 L-2012\2012P.dta", replace

/*selecting 2011 release data*/
insheet using "${datapath}\EU-SILC\7 L-2011\UDB_l11P_ver 2011-4 from 01-03-2015.csv", clear
tostring pb030, replace
rename pb010 year
rename pb030 pid   
rename pb020 country
sort year country pid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (P file) and drop households that are only in the D file, but not in the R file */
merge 1:1 year country pid using "${datapath}\EU-SILC\7 L-2011\2011slctdR.dta"
keep if _merge == 3 
drop _merge 
save "${datapath}\EU-SILC\7 L-2011\2011P.dta", replace

/*selecting 2010 release data*/
insheet using "${datapath}\EU-SILC\6 L-2010\UDB_l10P_ver 2010-5 from 01-08-2014.csv", clear
tostring pb030, replace
rename pb010 year
rename pb030 pid   
rename pb020 country
sort year country pid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (P file) and drop households that are only in the D file, but not in the R file */
merge 1:1 year country pid using "${datapath}\EU-SILC\6 L-2010\2010slctdR.dta"
keep if _merge == 3 
drop _merge 
save "${datapath}\EU-SILC\6 L-2010\2010P.dta", replace

/*selecting 2009 release data*/
insheet using "${datapath}\EU-SILC\5 L-2009\UDB_l09P_ver 2009-4 from 01-03-2013.csv", clear
tostring pb030, replace
rename pb010 year
rename pb030 pid   
rename pb020 country
sort year country pid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (P file) and drop households that are only in the D file, but not in the R file */
merge 1:1 year country pid using "${datapath}\EU-SILC\5 L-2009\2009slctdR.dta"
keep if _merge == 3 
drop _merge 
save "${datapath}\EU-SILC\5 L-2009\2009P.dta", replace

/*selecting 2008 release data*/
insheet using "${datapath}\EU-SILC\4 L-2008\UDB_l08P_ver 2008-4 from 01-03-2012.csv", clear
tostring pb030, replace
rename pb010 year
rename pb030 pid   
rename pb020 country
sort year country pid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (P file) and drop households that are only in the D file, but not in the R file */
merge 1:1 year country pid using "${datapath}\EU-SILC\4 L-2008\2008slctdR.dta"
keep if _merge == 3 
drop _merge 
save "${datapath}\EU-SILC\4 L-2008\2008P.dta", replace

/*selecting 2007 release data*/
insheet using "${datapath}\EU-SILC\3 L-2007\UDB_l07P_ver 2007-5 from 01-08-2011.csv", clear
tostring pb030, replace
rename pb010 year
rename pb030 pid   
rename pb020 country
sort year country pid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (P file) and drop households that are only in the D file, but not in the R file */
merge 1:1 year country pid using "${datapath}\EU-SILC\3 L-2007\2007slctdR.dta"
keep if _merge == 3 
drop _merge 
save "${datapath}\EU-SILC\3 L-2007\2007P.dta", replace

/*selecting 2006 release data*/
insheet using "${datapath}\EU-SILC\2 L-2006\UDB_L06P_ver 2006-2 from 01-03-2009.csv", clear
tostring pb030, replace
rename pb010 year
rename pb030 pid   
rename pb020 country
sort year country pid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (P file) and drop households that are only in the D file, but not in the R file */
merge 1:1 year country pid using "${datapath}\EU-SILC\2 L-2006\2006slctdR.dta"
keep if _merge == 3 
drop _merge 
save "${datapath}\EU-SILC\2 L-2006\2006P.dta", replace

/*selecting 2006 release data*/
insheet using "${datapath}\EU-SILC\1 L-2005\UDB_L05P_ver 2005-1 from 15-09-07.csv", clear
tostring pb030, replace
rename pb010 year
rename pb030 pid   
rename pb020 country
sort year country pid 
/*checking for duplicates/errors in hid*/
duplicates report year country pid
duplicates drop year country pid, force
/*merge with IDs from house hold register (P file) and drop households that are only in the D file, but not in the R file */
merge 1:1 year country pid using "${datapath}\EU-SILC\1 L-2005\2005slctdR.dta"
keep if _merge == 3 
drop _merge 
save "${datapath}\EU-SILC\1 L-2005\2005P.dta", replace

/*merge data from all releases with masterfile. 
!! this process is memory intensive. */
use "${datapath}\EU-SILC\9 L-2013\masterP.dta", clear
merge 1:1 year upid using "${datapath}\EU-SILC\8 L-2012\2012P.dta"
drop _merge
merge 1:1 year upid using "${datapath}\EU-SILC\7 L-2011\2011P.dta"
drop _merge
merge 1:1 year upid using "${datapath}\EU-SILC\6 L-2010\2010P.dta"
drop _merge
merge 1:1 year upid using "${datapath}\EU-SILC\5 L-2009\2009P.dta"
drop _merge
save "${datapath}\EU-SILC\9 L-2013\masterR.dta", replace
clear
use "${datapath}\EU-SILC\9 L-2013\masterR.dta"
merge 1:1 year upid using "${datapath}\EU-SILC\4 L-2008\2008P.dta"
drop _merge
merge 1:1 year upid using "${datapath}\EU-SILC\3 L-2007\2007P.dta"
drop _merge
save "${datapath}\EU-SILC\9 L-2013\masterR.dta", replace
clear
use "${datapath}\EU-SILC\9 L-2013\masterR.dta"
merge 1:1 year upid using "${datapath}\EU-SILC\2 L-2006\2006P.dta"
drop _merge
save "${datapath}\EU-SILC\9 L-2013\masterR.dta", replace
clear
use "${datapath}\EU-SILC\9 L-2013\masterR.dta"
merge 1:1 year upid using "${datapath}\EU-SILC\1 L-2005\2005P.dta"
drop _merge
/* This is the personal data file (D) file 2003 - 20013 */
save "${datapath}\EU-SILC\9 L-2013\masterP.dta", replace



/*erasing superflous files from the disc */
erase "${datapath}\EU-SILC\9 L-2013\2013slctd.dta"

erase "${datapath}\EU-SILC\8 L-2012\2012slctd.dta"
erase "${datapath}\EU-SILC\8 L-2012\2012slctdR.dta"
erase "${datapath}\EU-SILC\8 L-2012\2012D.dta"
erase "${datapath}\EU-SILC\8 L-2012\2012H.dta"
erase "${datapath}\EU-SILC\8 L-2012\2012P.dta"
erase "${datapath}\EU-SILC\8 L-2012\2012R.dta"

erase "${datapath}\EU-SILC\7 L-2011\2011slctd.dta"
erase "${datapath}\EU-SILC\7 L-2011\2011slctdR.dta"
erase "${datapath}\EU-SILC\7 L-2011\2011D.dta"
erase "${datapath}\EU-SILC\7 L-2011\2011H.dta"
erase "${datapath}\EU-SILC\7 L-2011\2011P.dta"
erase "${datapath}\EU-SILC\7 L-2011\2011R.dta"

erase "${datapath}\EU-SILC\6 L-2010\2010slctd.dta"
erase "${datapath}\EU-SILC\6 L-2010\2010slctdR.dta"
erase "${datapath}\EU-SILC\6 L-2010\2010D.dta"
erase "${datapath}\EU-SILC\6 L-2010\2010H.dta"
erase "${datapath}\EU-SILC\6 L-2010\2010P.dta"
erase "${datapath}\EU-SILC\6 L-2010\2010R.dta"

erase "${datapath}\EU-SILC\5 L-2009\2009slctd.dta"
erase "${datapath}\EU-SILC\5 L-2009\2009slctdR.dta"
erase "${datapath}\EU-SILC\5 L-2009\2009D.dta"
erase "${datapath}\EU-SILC\5 L-2009\2009H.dta"
erase "${datapath}\EU-SILC\5 L-2009\2009P.dta"
erase "${datapath}\EU-SILC\5 L-2009\2009R.dta"

erase "${datapath}\EU-SILC\4 L-2008\2008slctd.dta"
erase "${datapath}\EU-SILC\4 L-2008\2008slctdR.dta"
erase "${datapath}\EU-SILC\4 L-2008\2008D.dta"
erase "${datapath}\EU-SILC\4 L-2008\2008H.dta"
erase "${datapath}\EU-SILC\4 L-2008\2008P.dta"
erase "${datapath}\EU-SILC\4 L-2008\2008R.dta"

erase "${datapath}\EU-SILC\3 L-2007\2007slctd.dta"
erase "${datapath}\EU-SILC\3 L-2007\2007slctdR.dta"
erase "${datapath}\EU-SILC\3 L-2007\2007D.dta"
erase "${datapath}\EU-SILC\3 L-2007\2007H.dta"
erase "${datapath}\EU-SILC\3 L-2007\2007P.dta"
erase "${datapath}\EU-SILC\3 L-2007\2007R.dta"

erase "${datapath}\EU-SILC\2 L-2006\2006slctd.dta"
erase "${datapath}\EU-SILC\2 L-2006\2006slctdR.dta"
erase "${datapath}\EU-SILC\2 L-2006\2006D.dta"
erase "${datapath}\EU-SILC\2 L-2006\2006H.dta"
erase "${datapath}\EU-SILC\2 L-2006\2006P.dta"
erase "${datapath}\EU-SILC\2 L-2006\2006R.dta"

erase "${datapath}\EU-SILC\1 L-2005\2005slctd.dta"
erase "${datapath}\EU-SILC\1 L-2005\2005slctdR.dta"
erase "${datapath}\EU-SILC\1 L-2005\2005D.dta"
erase "${datapath}\EU-SILC\1 L-2005\2005H.dta"
erase "${datapath}\EU-SILC\1 L-2005\2005P.dta"
erase "${datapath}\EU-SILC\1 L-2005\2005R.dta"


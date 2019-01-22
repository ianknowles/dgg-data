## ------------------------------------------------------------------------- ##
## title: Data Preprocessing
## Descriptions: The code here reads in the dataset and defines the features
##      and other variables that are used in the analysis.
## Date: Updated 4 Feb 2018
## ------------------------------------------------------------------------- ##




## ------------------------------------------------------------------------- ##
## In this part the dataset is read in from a csv file. Variables used 
## in the analysis are defined here as well.
## ------------------------------------------------------------------------- ##

# read the dataset: years of publication, country codes and country names
# are read as 'factor' while everything else is read as 'numeric'
filename <- "../data/Digital_Gender_Gap_Dataset.csv"
columnTypes <- c(rep('factor',6),'numeric',rep('factor',7),
                 rep('numeric',129),rep('factor',2),rep('numeric',4))

# datas <- read.csv(filename, colClasses = columnTypes)
datas <- read.csv(filename)

# filter out countries that have FB or Internet population (reported by IWS)
# counts of 20 as these are likely noise
datas <- datas[datas$IntUsers_2017!=20 
               & datas$FbpopIWS_2016!=20 
               & datas$FB_18_plus_pop_2017!=20,]



# ~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_ #
# ---------- Variable Definition for variables used in the script.------------ #

# This is the age groups for which population and FB user sex ratios are 
# available. They are specified in the same format as they appear in the
# names of the variables in our dataset.
ageGroups = c("age_18_plus",
              "age_15_19","age_20_24","age_25_29",
              "age_30_34","age_35_39","age_40_44",
              "age_45_49","age_50_54","age_55_59",
              "age_60_64",
              "age_65_plus","age_18_23",
              "age_20_plus","age_20_64","age_21_plus",
              "age_25_plus","age_25_49","age_25_64",
              "age_50_plus","age_60_plus")

# Intuitive names for the above age groups. This is used only when printing
# out results.
ageGroups_names = c("18+",
                    "15-19","20-24","25-29","30-34",
                    "35-39","40-44","45-49","50-54",
                    "55-59","60-64","65+",
                    "18-23","20+","20-64","21+",
                    "25+","25-49","25-64","50+","60+")

# Names of the various device types for which we have number of FB users.
# These names are specified in the same format as they appear in the
# names of the variables in our dataset.
FBdevices <- c("android_device_users",
               "iOS_device_users",
               "mobile_device_users",
               "feature_phone_users",
               "iPhone7_users",
               "smartphone_owners")

# Intuitive names for the device types above. This is used only
# when printing the results
FBdevices_names <- c("Android device",
                     "iOS device",
                     "Mobile Phone",
                     "Feature Phone",
                     "iPhone 7",
                     "Smart Phone")





# ~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_ #
# ------------ Variable Definition for variables used in Modelling.----------- #

# ============================================================================= #
# The Facebook, population adjusted sex ratio for all age groups.
# ============================================================================= #
# These variables are defined as:
# (Facebook female to male sex ratio)/(population female to male ratio)
# where the numerator & denominator are for a specific age group specified in
# the name of the variable.
# This adjusts the sex ratio by the population sex ratio in order to adjust for 
# population imbalances in countries such as Qatar which have a higher proportion 
# of males. This ensures that the FB Gender Gaps measures gaps rather than 
# population imbalances.

for (i in 1:length(ageGroups)) {
  # for every age group adjust the FB sex ratio by the population sex ratio
  
  FB <- paste("FB_",ageGroups[i],"_ratio",sep = "")
  Off <- paste("Off_",ageGroups[i],"_ratio",sep = "")
  newcol <- paste("FB_",ageGroups[i],"_ratio_adjusted",sep = "")
  
  datas[newcol] <- datas[,colnames(datas) == FB]/datas[,colnames(datas) == Off]
}

# ============================================================================= #
# Facebook Gender Gap (GG) for various age groups.
# ============================================================================= #
# In line with the methodology of the Global Gender Gap report we cap the 
# adjusted Facebook sex ratio for each age group. 
# This is our measure of the Facebook Gender Gap for that age group

for (i in 1:length(ageGroups)) {
  
  # for every age group define the FB gender gap
  FB <- paste("FB_",ageGroups[i],"_ratio_adjusted",sep = "")
  newcol <- paste("FB_GG_",ageGroups[i],sep = "")
  
  datas[newcol] <- datas[,colnames(datas) == FB]
  I <- !is.na(datas[newcol]) & datas[newcol] > 1
  datas[newcol][I] <- 1
}

# ============================================================================= #
# The Facebook Mobile Gender Gaps.
# ============================================================================= #
# The FB gender gap for various device types, defined as:
# (FB female/male sex ratio for that device type)/(population female/male sex ratio)
# The ratio is capped at 1

for (i in 1:length(FBdevices)) {
  # define the FB Gender Gap for each device type
  
  FB <- paste("FB_",FBdevices[i],"_ratio",sep = "")
  newcol <- paste("FB_",FBdevices[i],"_GG",sep = "")
  
  datas[newcol] <- datas[,colnames(datas) == FB]/datas$Off_age_18_plus_ratio
  
  # cap at 1
  I <- !is.na(datas[newcol]) & datas[newcol] > 1
  datas[newcol][I] <- 1
}

# ============================================================================= #
# The Ground Truth Internet Gender Gap.
# ============================================================================= #
# This is the ground truth measure of the Internet gender gap. It is capped at 1
# in line with the methodology of the Global Gender Gap report.
datas$Internet_GG <- datas$ITU_IntAccess_ratio
I <- !is.na(datas$Internet_GG) & datas$Internet_GG > 1
datas$Internet_GG[I] <- 1

# ============================================================================= #
# The Ground Truth Mobile Gender Gap.
# ============================================================================= #
# This is the ground truth measure of the Mobile Gender Gap. It is capped at 1.
# The GSMA defines its gender gap as (male - female)/male
# But we define it as female/male
datas$Mobile_GG <- 1 - datas$Mob_ownership_GG_GSMA
I <- !is.na(datas$Mobile_GG) & datas$Mobile_GG > 1
datas$Mobile_GG[I] <- 1

# ============================================================================= #
# The Log(GDP per capita)
# ============================================================================= #
# This is the log transformation of the GDP per capita.
datas$log_GDP_capita <- log(datas$GDP_capita_PPP_2016)

# ============================================================================= #
# Adult Literacy rate and Secondary Education HDI
# ============================================================================= #
# Divides these values by 100 to convert them from percentages to 
# values in [0,1]
datas$Adult_literacy_HDI <- datas$Adult_literacy_HDI/100
datas$Sec_educ_HDI <- datas$Sec_educ_HDI/100

# ============================================================================= #
# The Unique Subscriber Penetration Indicator.
# ============================================================================= #
# whether or not a country has a low unique subscriber penetration defined as
# a unique subscriber penetration below 40%
datas$low_unique_subscriber_penetration <- datas$Unique_Subscriber_penetration < 0.4

# ============================================================================= #
# The Indicator for being in South Asia.
# ============================================================================= #
# Is this country in South Asia
datas$Is_South_Asia <- !is.na(datas$Region_WB) & datas$Region_WB == "South Asia"

# ============================================================================= #
# Facebook Penetration
# ============================================================================= #
# Defined as the Total Facebook population (18+) divided by the total 
# population of the country. May be greater than 1 due to mismatch of the
# dates at which the data is collected so it is capped at 1.
datas$FB_penentration <- datas$FB_18_plus_pop_2017/datas$PopulationIWS_2017

# ============================================================================= #
# Ratio of Facebook Penetration to Internet Penetration
# ============================================================================= #
datas$FB_Internet_penetration_ratio <- datas$FB_penentration/datas$Int_pentr_IWS

# ============================================================================= #
# A dataframe containing the ITU ground truth dataset. 
# ============================================================================= #
# NOTE: Our dataset has only 78 of the 84 countries in the ITU dataset. so we
# use this dataframe when making plots on maps or for reporting numbers about
# the coverage of countries in the ITU dataset.
I <- !is.na(datas$Internet_GG)
ITU_dataset <- data.frame(c(as.character(datas$Country[I]),
                            "Cuba",
                            "Iran",
                            "Montserrat",
                            "Palestine",
                            "Puerto Rico",
                            "Sudan"),
                          c(as.character(datas$ISO3Code[I]),
                            "CUB",
                            "IRN",
                            "MSR",
                            "PSE",
                            "PRI",
                            "SDN"),
                          c(datas$Internet_GG[I],1,0.959,1,0.797,1,0.547),
                          c(as.character(datas$Region_WB[I]), 
                            "Latin America and the Caribbean",
                            "Middle East and North Africa",
                            "",
                            "",
                            "Latin America and the Caribbean",
                            "Africa"),
                          c(as.character(datas$UN_M49_Class[I]),
                            "Developing",
                            "Developing",
                            "Developing",
                            "Developing",
                            "Developing",
                            "Developing"),
                          c(as.character(datas$WB_Income_Class[I]),
                            "Upper-Middle-Income",
                            "Upper-Middle-Income",
                            "",
                            "",
                            "High-Income",
                            "Lower-Middle-Income"))
colnames(ITU_dataset) <- c("Country","ISO3Code","Internet_GG",
                           "Region_WB","UN_M49_Class","WB_Income_Class")






# ~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_ #
# ---------- Grouping variables for ease of reference later.------------------ #

# List of names of variables in the dataset that are measures of development
# of a country.
devIndicators <- c("Int_pentr_IWS",
                   "GDP_capita_PPP_2016",
                   "log_GDP_capita",
                   "HDI",
                   "Adult_literacy_HDI",
                   "Educ_HDI",
                   "Mean_yr_school_HDI",
                   "Sec_educ_HDI",
                   "Unemployment_ratio",
                   "Income_HDI",
                   "Multidimensional_poverty_index",
                   "FB_penentration",
                   "FB_Internet_penetration_ratio")

# Intuitive names for above variables. Used only when printing the results.
devIndicators_names <- c("Internet Penetration",
                         "GDP per Capita 2016",
                         "log(GDP per Capita)",
                         "HDI",
                         "Adult Literacy rate (HDI)",
                         "HDI - Education",
                         "HDI - Mean Years of Schooling",
                         "HDI - Secondary Education Rate",
                         "HDI - Unemployment rate Sex ratio",
                         "HDI - Income",
                         "Oxford Multidimensional Poverty Index",
                         "FB penentration",
                         "FB/Internet penetration ratio")

# Names of variables in the dataset that are Gender Gap Indicators 
# as reported in the Global Gender Gap report
GGIndicators <- c("GGG_score",
                  "Econ_opp_GG_subindex",
                  "Labr_force_GG",
                  "Wage_equal_GG",
                  "Estim_income_GG",
                  "Seniority_GG",
                  "Professionality_GG",
                  "Educ_attn_GG_subindex",
                  "Literacy_GG",
                  "Enrol_prim_educ_GG",
                  "Enrol_sec_educ_GG",
                  "Enrol_ter_educ_GG",
                  "Health_GG_subindex",
                  "Birth_sex_ratio",
                  "Life_exp_GG",
                  "Political_empowerment_GG_subindex",
                  "Parliament_GG",
                  "Ministerial_GG",
                  "Female_head_GG")

# Intuitive names for the variables above. 
# Only used when printing the results
GGIndicators_names <- c("GG Score",
                        "GG - Economy",
                        "GG - Labour Force",
                        "GG - Wages",
                        "GG - Income",
                        "GG - Managerial Work",
                        "GG - Professional work",
                        "GG - Education",
                        "GG - Literacy",
                        "GG - Primary Educ.",
                        "GG - Secondary Educ.",
                        "GG - Tertiary Educ.",
                        "GG - Health",
                        "Sex Ratio at Birth",
                        "GG - Life Expectancy",
                        "GG - Politics",
                        "GG - Parliament",
                        "GG - Minsiterial",
                        "GG - Female Head of State")

# Names of variables in the dataset that are related to 
# country's mobile market 
mobileVars <- c("Unique_Subscriber_penetration",
                "low_unique_subscriber_penetration",
                "Is_South_Asia")

# Intuitive names for the above variables.
mobileVars_names <- c("Unique Subscriber penetration",
                      "low Unique Subscriber Penetration Indicator",
                      "South Asia Indicator")

# Names of variables in the dataset that have data for the 
# FB gender gaps of the various age groups.
FBAgeGG <- c("FB_GG_age_18_plus",
             "FB_GG_age_15_19","FB_GG_age_20_24","FB_GG_age_25_29",
             "FB_GG_age_30_34","FB_GG_age_35_39","FB_GG_age_40_44",
             "FB_GG_age_45_49","FB_GG_age_50_54","FB_GG_age_55_59",
             "FB_GG_age_60_64",
             "FB_GG_age_65_plus","FB_GG_age_18_23",
             "FB_GG_age_20_plus","FB_GG_age_20_64","FB_GG_age_21_plus",
             "FB_GG_age_25_plus","FB_GG_age_25_49","FB_GG_age_25_64",
             "FB_GG_age_50_plus","FB_GG_age_60_plus")

# Intuitive names for the above variables. 
FBAgeGG_names <- c("FB GG age 18+",
                   "FB GG age 15-19","FB GG age 20-24","FB GG age 25-29",
                   "FB GG age 30-34","FB GG age 35-39","FB GG age 40-44",
                   "FB GG age 45-49","FB GG age 50-54","FB GG age 55-59",
                   "FB GG age 60-64",
                   "FB GG age 65+","FB GG age 18-23",
                   "FB GG age 20+","FB GG age 20-64","FB GG age 21+",
                   "FB GG age 25+","FB GG age 25-49","FB GG age 25-64",
                   "FB GG age 50+","FB GG age 60+")

# Names of variables in the dataset that have data for the 
# FB gender gaps for the various device types.
FBDeviceGG <- c("FB_android_device_users_GG",
                "FB_iOS_device_users_GG",
                "FB_mobile_device_users_GG",
                "FB_feature_phone_users_GG",
                "FB_iPhone7_users_GG",
                "FB_smartphone_owners_GG")

# Intuitive names for the above variables. 
FBDeviceGG_names <- c("FB android device GG",
                      "FB iOS device GG",
                      "FB mobile device GG",
                      "FB feature phone GG",
                      "FB iPhone7 GG",
                      "FB Smart Phone GG")



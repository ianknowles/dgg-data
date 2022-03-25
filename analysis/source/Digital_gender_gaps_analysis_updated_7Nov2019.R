## ------------------------------------------------------------------------- ##
## title: Digital Gender Gaps Analysis
## Descriptions: Reads in the data and model files provided as input and
##      creates a table of predictions.
##
## Date: 7 Nov 2018
## ------------------------------------------------------------------------- ##



## ----
#  Script Parameters: Spceify the names of the sources of input and output files
## ----

## inputs
# Online data -- FB counts and ratios
args <- commandArgs(trailingOnly = TRUE)
input_FB_counts <- args[1]
FB_data_id_col <- "Country"

# Offline data -- Offline variables, GGI and HDI
input_offline_variables <-  "../input/DGG_Offline_dataset_compiled_Nov_2019.csv"
offline_data_id_col <- "two_digit_code" # or alternatively "ISO3Code" for three digit code

# Ground truth data column names 
# by default the latest data prior to the current year (year of FB data) is used (eg: if year is 2019 data from 2018 or earlier is used)
# if you want to over-ride this, replace NULL with name of desired data column to be used
internet_gg_gtruth_col <- NULL
mobile_gg_gtruth_col <- NULL

# Prediction Models
internet_GG_models_file <- "../models/Internet_GG_models_selected_by_stepwise_cv_smape.RData"
mobile_GG_models_file <- "../models/Mobile_GG_models_selected_by_stepwise_cv_smape.RData"

## output files
output_path <- args[2]
model_predictions_file <- file.path(output_path, "Appendix_table_model_predictions.csv")
correlations_file <- file.path(output_path, "GroundTruth_correlations_table.csv")
fits_file <- file.path(output_path, "fits.csv")

## ----
#  Helper functions
## ----

## input:
##   * datas: the dataset
##   * variable_base_name: the base name of the variable we are searching for
##        variables are assumed to have the name format 'variable_base_name_[year]'
##   * year: latest year for which we want data. eg: if year = 2019 then
##        searches and returns the latest year <= 2019
## output:
##  searches for the given variable in the dataset and returns the name
##  of the dataframe column that has data for the latest year prior to or
##  equal to the specified year.
##  if the variables is not found it return NULL
get_data_column_to_use <- function(datas, variable_base_name, year) {
  current_year <- as.numeric(year)
  
  I <- grepl(paste0("^", variable_base_name, "_[0-9]{4}"), colnames(datas))
  if (sum(I) == 0) { return(NULL)}
  
  # the variable exists in the dataframe; get the appropriate year of data
  years_of_data <- as.numeric(gsub(paste0("^", variable_base_name, "_([0-9]{4})"), "\\1", colnames(datas)[I]))
  years_of_data <- years_of_data[years_of_data <= current_year]
  
  if (length(years_of_data) == 0) { return(NULL)} # no data before current year
  else { return(paste(variable_base_name, max(years_of_data), sep="_")) } # return latest year of data
  
}

# computes the SMAPE error metric between the predictions ('pred_col') and
# ground truth ('gtruth_col') variables in the 'pred_df'
smape_error <- function(pred_df, gtruth_col, pred_col) {
  I <- !is.na(pred_df[,gtruth_col]) & !is.na(pred_df[,pred_col])
  gtruth <- pred_df[I,gtruth_col]
  pred <- pred_df[I,pred_col]
  
  error <- abs(gtruth - pred)
  error <- error/((abs(gtruth)+abs(pred))/2)
  error <- sum(error)/length(pred)
  
  return(error)
}

# computes the Mean Absolute Error (MAE) metric between the predictions ('pred_col') and
# ground truth ('gtruth_col') variables in the 'pred_df'
mae_error <- function(pred_df, gtruth_col, pred_col) {
  I <- !is.na(pred_df[,gtruth_col]) & !is.na(pred_df[,pred_col])
  gtruth <- pred_df[I,gtruth_col]
  pred <- pred_df[I,pred_col]
  
  return(mean(abs(gtruth - pred)))
}

# computes the R^2 metric between the predictions ('pred_col') and
# ground truth ('gtruth_col') variables in the 'pred_df'
r.squared <- function(pred_df, gtruth_col, pred_col) {
  I <- !is.na(pred_df[,gtruth_col]) & !is.na(pred_df[,pred_col])
  gtruth <- pred_df[I,gtruth_col]
  pred <- pred_df[I,pred_col]
  resids <- gtruth - pred
  
  RSS <- sum(resids^2)
  TSS <- sum((gtruth - mean(gtruth))^2)
  return(1 - RSS/TSS)
}

## ----
#  Setup script variables
## ----
# FB age groups
ageGroups <- c("age_18_plus",
               "age_15_19", "age_20_24", "age_25_29",
               "age_30_34", "age_35_39", "age_40_44",
               "age_45_49", "age_50_54", "age_55_59",
               "age_60_64",
               "age_65_plus", "age_18_23",
               "age_20_plus", "age_20_64", "age_21_plus",
               "age_25_plus", "age_25_49", "age_25_64",
               "age_50_plus", "age_60_plus")

# FB device users
FBdevices <- c("android_device_users",
               "iOS_device_users",
               "mobile_device_users",
               "feature_phone_users",
               "iPhone7_users",
               "smartphone_owners")

# base name of the offline sex ratio variables before the year of the data
offline_sex_ratio_variables <- paste("Off", ageGroups, "ratio_rev", sep = "_")

# Development indicators
devIndicators <- c("Int_pentr_IWS",
                   "GDP_capita_PPP",
                   "log_GDP_capita",
                   "HDI",
                   "Adult_literacy_HDI",
                   "Educ_HDI",
                   "Mean_yr_school_HDI",
                   "Sec_educ_HDI",
                   "Unemployment_ratio",
                   "Income_HDI",
                   "Multidimensional_poverty_index") #"FB_penetration", "FB_Internet_penetration_ratio")

# Global Gender Gap report GGI
GGIndicators_capped <- c("GGG_score",
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

GGcomposite_indices <- c("GGG_score","Econ_opp_GG_subindex","Educ_attn_GG_subindex","Health_GG_subindex","Political_empowerment_GG_subindex")
GGIndicators_uncapped <- c(GGcomposite_indices,
                           paste(GGIndicators_capped[!is.element(GGIndicators_capped, GGcomposite_indices)], "raw",sep="_"))



## ----
#  Data Pre-processing - merge data and generate features
## ----

## read in and merge the input datasets
FB_data <- read.csv(input_FB_counts, stringsAsFactors = FALSE)
offline_data <- read.csv(input_offline_variables, stringsAsFactors = FALSE)
I <- offline_data$country == "Namibia"
offline_data$two_digit_code[I] <- "NA"

print("________ Reading in and Merging input data files ________")
print(paste("Online data:", input_FB_counts, "Offline data:", input_offline_variables))
print(paste("FB data # of rows:",nrow(FB_data)))
print(paste("Offline data # of rows:",nrow(offline_data)))
print(paste("Number of data points sucessfully matched:",
            sum(is.element(offline_data[,offline_data_id_col], FB_data[,FB_data_id_col]))))

datas <- merge(x = offline_data, y = FB_data,
               by.x = offline_data_id_col, by.y = FB_data_id_col, all.x = TRUE)

## Determine which year of data to use for the variables of interest
# year is inferred from the time-stamp on the FB data 
current_year <- as.numeric(gsub(".*_counts_([0-9]{4})-[0-9]{2}-[0-9]{2}.csv", "\\1", input_FB_counts))

# for Offline sex ratios we used data from current year
for (varName in c(offline_sex_ratio_variables)) { 
  varName_with_year <- get_data_column_to_use(datas, varName, current_year)
  print(paste("Variable base name:", varName, "print name with year:", varName_with_year))
  if (!is.null(varName_with_year)) { 
    # create a data column with data from the chosen year
    datas[varName] <- datas[,varName_with_year]
    
  }
}

# for Offline variables we use lagged data from previou year
for (varName in c(devIndicators, GGIndicators_uncapped)) { 
  varName_with_year <- get_data_column_to_use(datas, varName, current_year-1)
  # print(paste("Variable base name:", varName, "print name with year:", varName_with_year))
  if (!is.null(varName_with_year)) { 
    # create a data column with data from the chosen year
    datas[varName] <- datas[,varName_with_year]
    
  }
}

# For the ground truth data also we use the lagged data from previous year
if (is.null(internet_gg_gtruth_col)) {
  internet_gg_gtruth_col <- get_data_column_to_use(datas, "ITU_IntAccess_ratio", current_year-1)
  if (is.null(internet_gg_gtruth_col)) {
    print("Could not find column for ground truth Internet GGI data.")
    stop()
  }
}

if (is.null(mobile_gg_gtruth_col)) {
  mobile_gg_gtruth_col <- get_data_column_to_use(datas, "mobile_gg_latest", current_year-1)
  if (is.null(mobile_gg_gtruth_col)) {
    print("Could not find column for ground truth Mobile GGI data.")
    stop()
  }
}





## generate features for the prediction
# FB age-sepcific gender gap indices -- capped and uncapped variants
FBAgeGG_capped <- c()
FBAgeGG_uncapped <- c()

for (age in ageGroups) {
  FB <- paste("FB",age,"ratio", sep="_")
  off <- paste("Off",age,"ratio_rev", sep="_")
  FBGG_capped <- paste("FB_GG",age,"capped", sep="_")
  FBGG_uncapped <- paste("FB_GG",age,"uncapped", sep="_")
  
  datas[FBGG_uncapped] <- datas[,FB]/datas[,off]
  datas[FBGG_capped] <- pmin(datas[,FBGG_uncapped],1)

  FBAgeGG_uncapped <- c(FBAgeGG_uncapped, FBGG_uncapped)
  FBAgeGG_capped <- c(FBAgeGG_capped, FBGG_capped)
}

# FB device-sepcific gender gap indices -- capped and uncapped variants
FBdevGG_capped <- c()
FBdevGG_uncapped <- c()
for (device in FBdevices) {
  FB <- paste("FB",device,"ratio", sep="_")
  FBGG_capped <- paste("FB",device,"GG_capped", sep="_")
  FBGG_uncapped <- paste("FB",device,"GG_uncapped", sep="_")
  
  datas[FBGG_uncapped] <- datas[,FB]/datas$Off_age_18_plus_ratio_rev
  datas[FBGG_capped] <- pmin(datas[,FBGG_uncapped],1)
  
  FBdevGG_uncapped <- c(FBdevGG_uncapped, FBGG_uncapped)
  FBdevGG_capped <- c(FBdevGG_capped, FBGG_capped)
}

# Ground truth GGI - Internet GGI capped/uncapped variants
datas$Internet_GG_uncapped <- datas[,internet_gg_gtruth_col]
datas$Internet_GG_capped <- pmin(datas$Internet_GG_uncapped,1)

# Ground truth GGI - Mobile GGI capped/uncapped variants 
datas$Mobile_GG_uncapped <- datas[,mobile_gg_gtruth_col]
datas$Mobile_GG_capped <- pmin(datas$Mobile_GG_uncapped,1)

## Other offline features
# log GDP per capita
datas$log_GDP_capita <- log(datas[,"GDP_capita_PPP"])

# # education HDI
# datas$Adult_literacy_HDI <- datas$Adult_literacy_HDI/100
# datas$Sec_educ_HDI <- datas$Sec_educ_HDI/100
# 
# # Facebook Penetration
# datas$FB_penetration <- datas[,FB_18_plus_pop]/datas[,PopulationIWS]
# 
# # Ratio of Facebook Penetration to Internet Penetration
# datas$FB_Internet_penetration_ratio <- datas$FB_penetration/datas$Int_pentr_IWS
# I <- datas$Int_pentr_IWS == 0
# datas$FB_Internet_penetration_ratio[I] <- NA




## ----
#   Create a table of correlations with the ground truth variables
## ----
varlist <-c("Internet_GG_uncapped", "Mobile_GG_uncapped", 
            FBAgeGG_uncapped, FBdevGG_uncapped, 
            devIndicators[is.element(devIndicators, colnames(datas))], 
            GGIndicators_uncapped[is.element(GGIndicators_uncapped, colnames(datas))])

Paper_correlations_table <- data.frame(row.names = varlist)

# how many countries is each variable available for?
# Both in aggregate as well as when broken down by 
# the income/development class of the counry
Paper_correlations_table$NumCountries <- 0
for (varName in varlist) {
  Paper_correlations_table[varName,"NumCountries"] <- sum(!is.na(datas[,varName]))
  
  # How many Least Developed?
  I <- !is.na(datas[,varName]) & datas$UN_M49_Class == "Least Developed"
  Paper_correlations_table[varName,"Least_Developed"] <- sum(I)
  
  # How many Developing?
  I <- !is.na(datas[,varName]) & datas$UN_M49_Class == "Developing"
  Paper_correlations_table[varName,"Developing"] <- sum(I)
  
  # How many Developed?
  I <- !is.na(datas[,varName]) & datas$UN_M49_Class == "Developed"
  Paper_correlations_table[varName,"Developed"] <- sum(I)
  
  # How many Low-Income?
  I <- !is.na(datas[,varName]) & datas$WB_Income_Class == "Low-Income"
  Paper_correlations_table[varName,"Low_Income"] <- sum(I)
  
  # How many Lower-Middle-Income?
  I <- !is.na(datas[,varName]) & datas$WB_Income_Class == "Lower-Middle-Income"
  Paper_correlations_table[varName,"Lower_Middle_Income"] <- sum(I)
  
  # How many Upper-Middle-Income?
  I <- !is.na(datas[,varName]) & datas$WB_Income_Class == "Upper-Middle-Income"
  Paper_correlations_table[varName,"Upper_middle_Income"] <- sum(I)
  
  # How many High-Income?
  I <- !is.na(datas[,varName]) & datas$WB_Income_Class == "High-Income"
  Paper_correlations_table[varName,"High_Income"] <- sum(I)
}

# Correlations with the Internet and Mobile GG
groundTruth <- c("Internet_GG","Mobile_GG")
for (varName in varlist) {
  Paper_correlations_table[varName, paste("Internet_GG Number of countries")] <- 
    sum(!is.na(datas[,"Internet_GG_uncapped"]) & !is.na(datas[,varName]))
  if (Paper_correlations_table[varName, paste("Internet_GG Number of countries")] > 0) {
    Paper_correlations_table[varName, paste("Internet_GG Correlations")] <- 
      cor(datas[,varName], datas[,"Internet_GG_uncapped"], use="complete.obs")
  }
 
  Paper_correlations_table[varName, paste("Mobile_GG Number of countries")] <- 
    sum(!is.na(datas[,"Mobile_GG_uncapped"]) & !is.na(datas[,varName]))
  if (Paper_correlations_table[varName, paste("Mobile_GG Number of countries")] > 0) {
    Paper_correlations_table[varName, paste("Mobile_GG Correlations")] <- 
      cor(datas[,varName], datas[,"Mobile_GG_uncapped"], use="complete.obs")
  }
  
}

# 
write.csv(Paper_correlations_table, correlations_file)
print(paste("correlations file:", correlations_file))

## ----
#  Create table of predictions 
#  Also saves the fit metrics for these predictions relative to available ground truth
## ----

vv <- load(internet_GG_models_file)
internet_GG_models <- get(vv)
vv <- load(mobile_GG_models_file)
mobile_GG_models <- get(vv)
rm(list = c(vv))

digits <- 3 # number of significant digits for data.
Appendix_table <- data.frame(Country = datas$country,
                             ISO3Code = datas$ISO3Code,
                             ISO2Code = datas$two_digit_code,
                             "Ground Truth Internet GG" = signif(datas$Internet_GG_capped,digits),
                             check.names = FALSE)

fits <- NULL # keep fit statistics for the predictions made

## Internet GG model predictions
print("____________ Generating predictions for Internet GGI. ________________")
for (modelName in names(internet_GG_models)) {
  pred_col <- paste("Internet", modelName, "model prediction", sep=" ")
  Appendix_table[pred_col] <- signif(predict(internet_GG_models[[modelName]], datas),digits)
  
  print(paste("## Prediction summary statistic, Internet GGI model:", modelName))
  print(summary(Appendix_table[,pred_col]))
  print(" -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -")
  
  Appendix_table[,pred_col] <- pmin(pmax(Appendix_table[,pred_col],0),1) # predictions truncated to [0,1]
  
  tmpfit <- data.frame(Model = paste("Internet",modelName))
  tmpfit$n_samples <- sum(!is.na(Appendix_table[,pred_col]) & !is.na(Appendix_table[,"Ground Truth Internet GG"]))
  tmpfit$Mean_abs_error <- mae_error(Appendix_table, "Ground Truth Internet GG", pred_col)
  tmpfit$smape <- smape_error(Appendix_table, "Ground Truth Internet GG", pred_col)
  tmpfit$r.squared <- r.squared(Appendix_table, "Ground Truth Internet GG", pred_col)
  fits <- rbind(fits, tmpfit)
}

## Mobile GG model predictions
Appendix_table["Ground Truth Mobile GG"] <- signif(datas$Mobile_GG_capped,digits)

print("____________ Generating predictions for Mobile GGI. ________________")
for (modelName in names(mobile_GG_models)) {
  pred_col <- paste("Mobile", modelName, "model prediction", sep=" ")
  Appendix_table[pred_col] <- signif(predict(mobile_GG_models[[modelName]], datas),digits)
  
  print(paste("## Prediction summary statistic, Mobile GGI model:", modelName))
  print(summary(Appendix_table[,pred_col]))
  print(" -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -")
  
  Appendix_table[,pred_col] <- pmin(pmax(Appendix_table[,pred_col],0),1) # predictions truncated to [0,1]
  
  tmpfit <- data.frame(Model = paste("Mobile",modelName))
  tmpfit$n_samples <- sum(!is.na(Appendix_table[,pred_col]) & !is.na(Appendix_table[,"Ground Truth Mobile GG"]))
  tmpfit$Mean_abs_error <- mae_error(Appendix_table, "Ground Truth Mobile GG", pred_col)
  tmpfit$smape <- smape_error(Appendix_table, "Ground Truth Mobile GG", pred_col)
  tmpfit$r.squared <- r.squared(Appendix_table, "Ground Truth Mobile GG", pred_col)
  fits <- rbind(fits, tmpfit)
}

write.csv(Appendix_table, model_predictions_file)
write.csv(fits, fits_file)
print(paste("model predictions file:", model_predictions_file, "fits file:", fits_file))

print("_____________ Global Coverage of Predictions; # of countries predicted ____________________")
print(colSums(!is.na(Appendix_table[,-c(1,2)])))

print("_____________ Predictions error statistics: ____________________")
print(fits)
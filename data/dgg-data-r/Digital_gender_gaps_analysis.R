## ------------------------------------------------------------------------- ##
## title: Digital Gender Gaps Analysis
## Descriptions: This is the code for all the analysis that was done. It 
##      contains code for the data modeling as well as all tables and figures
##      that appear in the manuscript as of the last date this script was 
##      updated.
## Dependencies: This script depends on two other scripts namely
##      "Data_preprocessing.R" and "functions.R" and also on the dataset
##      "Digital_Gender_Gap_Dataset.csv". It also depends on the R libraries
##      that are loaded below.
##
## Date: Updated 4 Feb 2018
## ------------------------------------------------------------------------- ##

## Required libraries
library(rworldmap)  # for maps
library(boot) # for bootstrap
library(pls) # for PCA
library(reshape) # for aggregating the new data points 
library(ggplot2)
library(stringr)
library(broom)

## Source the R scripts for data preprocessing and the required functions
## setwd("/Users/mjf6/Digital Gender Gap Code and Data_Feb_2018") # modify this appropriately
source("Data_preprocessing.R")
source("functions.R")







## ------------------------------------------------------------------------- ##
# Correlation Analysis with the Ground Truth Internet and Mobile Gender Gaps
# for the variables in the dataset. Also a breakdown of data availability
# for each variables by development status of countries.
## ------------------------------------------------------------------------- ##

# This is the names of the variables (as they appear in the dataset) whose 
# correlations with the Internet Gender Gap we would like to compute
variables <- c("Internet_GG","Mobile_GG",
               devIndicators,mobileVars,
               GGIndicators,
               FBAgeGG,FBDeviceGG)

# This is a human readable name for the variables above which will be used when
# printing out the correlations table
variablesNames <- c("Internet_GG","Mobile_GG",
                    devIndicators_names,mobileVars_names,
                    GGIndicators_names,
                    FBAgeGG_names,FBDeviceGG_names)
# name of the csv file to print the results to (include the .csv extension)
file <- "../data/GroundTruth_correlations_table.csv"

# ~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_ #
# compute and store in this data frame the correlations of the variables with
# the ground truth internet and mobile gender gaps as well as the number of
# countries used in the computations.
Paper_correlations_table <- data.frame(row.names = variablesNames)

# how many countries is each variable available for?
# Both in aggregate as well as when broken down by 
# the income/development class of the counry
Paper_correlations_table$NumCountries <- 0
for (i in 1:length(variables)) {
  I <- !is.na(datas[,names(datas) == variables[i]])
  Paper_correlations_table$NumCountries[i] <- sum(I)
  
  # How many Least Developed?
  I <- !is.na(datas[,names(datas) == variables[i]]) & 
    datas$UN_M49_Class == "Least Developed"
  Paper_correlations_table$Least_Developed[i] <- sum(I)
  
  # How many Developing?
  I <- !is.na(datas[,names(datas) == variables[i]]) & 
    datas$UN_M49_Class == "Developing"
  Paper_correlations_table$Developing[i] <- sum(I)
  
  # How many Developed?
  I <- !is.na(datas[,names(datas) == variables[i]]) & 
    datas$UN_M49_Class == "Developed"
  Paper_correlations_table$Developed[i] <- sum(I)
  
  # How many Low-Income?
  I <- !is.na(datas[,names(datas) == variables[i]]) & 
    datas$WB_Income_Class == "Low-Income"
  Paper_correlations_table$Low_Income[i] <- sum(I)
  
  # How many Lower-Middle-Income?
  I <- !is.na(datas[,names(datas) == variables[i]]) & 
    datas$WB_Income_Class == "Lower-Middle-Income"
  Paper_correlations_table$Lower_Middle_Income[i] <- sum(I)
  
  # How many Upper-Middle-Income?
  I <- !is.na(datas[,names(datas) == variables[i]]) & 
    datas$WB_Income_Class == "Upper-Middle-Income"
  Paper_correlations_table$Upper_middle_Income[i] <- sum(I)
  
  # How many High-Income?
  I <- !is.na(datas[,names(datas) == variables[i]]) & 
    datas$WB_Income_Class == "High-Income"
  Paper_correlations_table$High_Income[i] <- sum(I)
}

# Correlations with the Internet and Mobile GG
groundTruth <- c("Internet_GG","Mobile_GG")
for (i in 1:length(groundTruth)) {
  results <- compute_Correlations_with_ground_truth(groundTruth[i], variables,
                                                    VariablesNames, dataset = datas)
  print(results)
  Paper_correlations_table[paste(groundTruth[i],"Correlations")] <- results$Correlations
  Paper_correlations_table[paste(groundTruth[i],"Number of countries")] <- results$NumCountries
}

# 
write.csv(Paper_correlations_table,file)












## ------------------------------------------------------------------------- ##
## Results of Internet Gender Gap Regression Analysis
## ------------------------------------------------------------------------- ##


##                  The Internet Online Regression Model
## ------------------------------------------------------------------------- ##
# This is a single variable model using the FB age 18+ gender gap as predictor
# Model is fit with standardized variables for ease of interpretation of 
# model coefficients
I <- !is.na(datas$Internet_GG) & !is.na(datas$FB_GG_age_18_plus)
Internet_on_modelData <- data.frame(Internet_GG = datas$Internet_GG[I], 
                                    FB_GG_age_18_plus = scale(datas$FB_GG_age_18_plus[I]))

Internet_online_model <- lm(Internet_GG ~ FB_GG_age_18_plus, Internet_on_modelData)
print("The Internet GG Online Model")
summary(Internet_online_model)
Internet_online_model_coefs <- tidy(Internet_online_model)
Internet_online_model_fit <- glance(Internet_online_model)
Internet_online_model_fit[1, "Model"] <- "Internet_online_model"

Mean_abs_error <- mean(abs(residuals(Internet_online_model)))
print(paste("Model Mean Absolute Error:",Mean_abs_error))
Internet_online_model_fit[1, "Mean_abs_error"] <- Mean_abs_error

# Compute Bootstrap standard errors of the coefficient estimates
cat("\n\n Bootstrap Estimates of regression coefficient standard errors:\n")
bootestim <- boot(data = Internet_on_modelData, statistic = boot.fn, 
                  R = 1000, responseVar = "Internet_GG")
cat("model variables and coefficients:")
bootestim$t0
bootestim
cat("\n\n\n")

boot.ci(bootestim, index = 1, conf = 0.999) # try conf 0.999 or 0.99 or 0.95 etc.
boot.ci(bootestim, index = 2, conf = 0.999)


# Fit model with Leave-One-Out Cross Validation (LOOCV) so as to estimate a
# measure of out of sample predictive performance
I <- !is.na(datas$Internet_GG) & !is.na(datas$FB_GG_age_18_plus)
Internet_on_modelData <- data.frame(Internet_GG = datas$Internet_GG[I], 
                                    FB_GG_age_18_plus = datas$FB_GG_age_18_plus[I])

print("Results of LOOCV for testing predictive power")
pred <- predictLOOCV(dataset = Internet_on_modelData, response = "Internet_GG",
                     indicators = "")
print("The SMAPE:")
error <- abs(Internet_on_modelData$Internet_GG - pred)
error <- error/((abs(Internet_on_modelData$Internet_GG)+abs(pred))/2)
error <- sum(error)/length(pred)
print(error)
Internet_online_model_fit[1, "smape"] <- error

write.csv(Internet_online_model_coefs, "../data/Internet_online_model_coefs.csv")
write.csv(Internet_online_model_fit, "../data/Internet_online_model_fit.csv")




##                  The Internet Online-Offline Regression Model
## ------------------------------------------------------------------------- ##
# setup the variables to be passed to the function which runs a 
# greedy step-wise forward algorithm to build this model
# --- Parameters
baseModel <- data.frame(datas$Internet_GG)
names(baseModel) <- c("Internet_GG")
varyName <- "Internet_GG"
candidVars_names <- c(devIndicators,
                      GGIndicators,
                      "FB_GG_age_18_plus")
candidVars <- datas[,is.element(colnames(datas),candidVars_names)]
minCountries <- 70

# Call a function to perform greedy step-wise forward for model selection
finModel_variables <- build_model_with_greedy_stepwise_forward(
  baseModel, varyName, candidVars, minCountries)
Internet_Online_Offline_model <- lm(Internet_GG ~., finModel_variables)
print("The Internet GG Online Offline Model")
summary(Internet_Online_Offline_model)
Internet_Online_Offline_model_coefs <- tidy(Internet_Online_Offline_model)
Internet_Online_Offline_model_fit <- glance(Internet_Online_Offline_model)
Internet_Online_Offline_model_fit[1, "Model"] <- "Internet_Online_Offline_model"

Mean_abs_error <- mean(abs(residuals(Internet_Online_Offline_model)))
print(paste("Model Mean Absolute Error:",Mean_abs_error))
Internet_Online_Offline_model_fit[1, "Mean_abs_error"] <- Mean_abs_error

# For the purpose of reporting in the paper the model chosen above is refit
# with standardized variables for each of interpretation of coefficients
I <- !is.na(datas$Internet_GG) &
  !is.na(datas$FB_GG_age_18_plus) &
  !is.na(datas$log_GDP_capita) &
  !is.na(datas$Literacy_GG) &
  !is.na(datas$Educ_attn_GG_subindex)

Internet_onoff_modelData <- data.frame(Internet_GG = datas$Internet_GG[I],
                                       FB_GG_age_18_plus = scale(datas$FB_GG_age_18_plus[I]),
                                       log_GDP_capita = scale(datas$log_GDP_capita[I]),
                                       Literacy_GG = scale(datas$Literacy_GG[I]),
                                       Educ_attn_GG_subindex = scale(datas$Educ_attn_GG_subindex[I]))

Internet_Online_Offline_model <- lm(Internet_GG ~., Internet_onoff_modelData)
summary(Internet_Online_Offline_model)
# TODO

# Bootstrap estimates of the coefficient standard errors
cat("\n\n Bootstrap Estimates of regression coefficient standard errors:\n")
bootestim <- boot(data = Internet_onoff_modelData, statistic = boot.fn, 
                  R = 1000, responseVar = "Internet_GG")
cat("model variables and coefficients:")
bootestim$t0
bootestim
cat("\n\n\n")

boot.ci(bootestim, index = 1, conf = 0.99)
boot.ci(bootestim, index = 1, conf = 0.999)
boot.ci(bootestim, index = 2, conf = 0.999)
boot.ci(bootestim, index = 3, conf = 0.95)
boot.ci(bootestim, index = 4, conf = 0.95)
boot.ci(bootestim, index = 5, conf = 0.95)


# Fit model using LOOCV so as to estimate out of sample prediction error
I <- !is.na(datas$Internet_GG) &
  !is.na(datas$FB_GG_age_18_plus) &
  !is.na(datas$log_GDP_capita) &
  !is.na(datas$Literacy_GG) &
  !is.na(datas$Educ_attn_GG_subindex)

Internet_onoff_modelData <- data.frame(Internet_GG = datas$Internet_GG[I],
                                       FB_GG_age_18_plus = datas$FB_GG_age_18_plus[I],
                                       log_GDP_capita = datas$log_GDP_capita[I],
                                       Literacy_GG = datas$Literacy_GG[I],
                                       Educ_attn_GG_subindex = datas$Educ_attn_GG_subindex[I])

cat("the LOOCV Results:\n")
pred <- predictLOOCV(dataset = Internet_onoff_modelData, response = "Internet_GG",
                     indicators = "")
print("The SMAPE:")
error <- abs(Internet_onoff_modelData$Internet_GG - pred)
error <- error/((abs(Internet_onoff_modelData$Internet_GG)+abs(pred))/2)
error <- sum(error)/length(pred)
print(error)
Internet_Online_Offline_model_fit[1, "smape"] <- error

write.csv(Internet_Online_Offline_model_coefs, "../data/Internet_Online_Offline_model_coefs.csv" )
write.csv(Internet_Online_Offline_model_fit, "../data/Internet_Online_Offline_model_fit.csv" )

fits <- rbind(Internet_online_model_fit, Internet_Online_Offline_model_fit)

write.csv(fits, "../data/fit.csv" )


##                  The Internet Offline Regression Model
## ------------------------------------------------------------------------- ##
# choose the model variables using the greedy-stepwise-forward 
# variable selection procedure
# --- Parameters to be passed to the model fitting algorithm
baseModel <- data.frame(datas$Internet_GG)
names(baseModel) <- c("Internet_GG")
varyName <- "Internet_GG"
candidVars_names <- c(devIndicators[
  !is.element(devIndicators,c("FB_Internet_penetration_ratio","FB_penetration"))],
  GGIndicators)
candidVars <- datas[,is.element(colnames(datas),candidVars_names)]
minCountries <- 70

finModel_variables <- build_model_with_greedy_stepwise_forward(
  baseModel, varyName, candidVars, minCountries)
Internet_Offline_model <- lm(Internet_GG ~., finModel_variables)
print("The Internet GG Offline Model")
summary(Internet_Offline_model)
Internet_Offline_model_coefs <- tidy(Internet_Offline_model)
Internet_Offline_model_fit <- glance(Internet_Offline_model)
Internet_Offline_model_fit[1, "Model"] <- "Internet_Offline_model"

Mean_abs_error <- mean(abs(residuals(Internet_Offline_model)))
print(paste("Model Mean Absolute Error:",Mean_abs_error))
Internet_Offline_model_fit[1, "Mean_abs_error"] <- Mean_abs_error

# Refit above model with standardized values
# for ease of interpretation of model coefficients
I <- !is.na(datas$Internet_GG) &
  !is.na(datas$Int_pentr_IWS) &
  !is.na(datas$Enrol_ter_educ_GG) &
  !is.na(datas$Econ_opp_GG_subindex) &
  !is.na(datas$GGG_score)

Internet_off_modelData <- data.frame(Internet_GG = datas$Internet_GG[I],
                                     Int_pentr_IWS = scale(datas$Int_pentr_IWS[I]),
                                     Enrol_ter_educ_GG = scale(datas$Enrol_ter_educ_GG[I]),
                                     Econ_opp_GG_subindex = scale(datas$Econ_opp_GG_subindex[I]),
                                     GGG_score = scale(datas$GGG_score[I]))

Internet_Offline_model <- lm(Internet_GG ~., Internet_off_modelData)
print("The Internet GG Offline Model")
summary(Internet_Offline_model)
#TODO

# Bootstrap estimates of the coefficient standard errors
bootestim <- boot(data = Internet_off_modelData, statistic = boot.fn, 
                  R = 1000, responseVar = "Internet_GG")
cat("model variables and coefficients:")
bootestim$t0
bootestim

boot.ci(bootestim, index = 1, conf = 0.999)
boot.ci(bootestim, index = 2, conf = 0.999)
boot.ci(bootestim, index = 3, conf = 0.95)
boot.ci(bootestim, index = 4, conf = 0.99)
boot.ci(bootestim, index = 5, conf = 0.95)

# fit model with LOOCV to make estimates of out of sample prediction error
I <- !is.na(datas$Internet_GG) &
  !is.na(datas$Int_pentr_IWS) &
  !is.na(datas$Enrol_ter_educ_GG) &
  !is.na(datas$Econ_opp_GG_subindex) &
  !is.na(datas$GGG_score)

Internet_off_modelData <- data.frame(Internet_GG = datas$Internet_GG[I],
                                     Int_pentr_IWS = datas$Int_pentr_IWS[I],
                                     Enrol_ter_educ_GG = datas$Enrol_ter_educ_GG[I],
                                     Econ_opp_GG_subindex = datas$Econ_opp_GG_subindex[I],
                                     GGG_score = datas$GGG_score[I])

pred <- predictLOOCV(dataset = Internet_off_modelData, response = "Internet_GG",
                     indicators = "")

print("The SMAPE:")
error <- abs(Internet_off_modelData$Internet_GG - pred)
error <- error/((abs(Internet_off_modelData$Internet_GG)+abs(pred))/2)
error <- sum(error)/length(pred)
print(error)
Internet_Offline_model_fit[1, "smape"] <- error

write.csv(Internet_Offline_model_coefs, "../data/Internet_Offline_model_coefs.csv" )
write.csv(Internet_Offline_model_fit, "../data/Internet_Offline_model_fit.csv" )

fits <- rbind(fits, Internet_Offline_model_fit)

write.csv(fits, "../data/fit.csv" )






## ------------------------------------------------------------------------- ##
## Results of Mobile Gender Gap Regression Analysis
## ------------------------------------------------------------------------- ##


##                  The Mobile Online Regression Model
## ------------------------------------------------------------------------- ##
# This is a single variable model fit on the most predictive Facebook gender 
# gap index. The model variables are standardized
I <- !is.na(datas$Mobile_GG) & !is.na(datas$FB_GG_age_25_29) 

Mobile_on_modelData = data.frame(Mobile_GG = datas$Mobile_GG[I],
                                 FB_GG_age_25_29 = scale(datas$FB_GG_age_25_29[I]))
Mobile_Online_model <- lm(Mobile_GG ~ FB_GG_age_25_29,Mobile_on_modelData)
print("The Mobile GG Online Model")
summary(Mobile_Online_model)
Mobile_Online_model_coefs <- tidy(Mobile_Online_model)
Mobile_Online_model_fit <- glance(Mobile_Online_model)
Mobile_Online_model_fit[1, "Model"] <- "Mobile_Online_model"

Mean_abs_error <- mean(abs(residuals(Mobile_Online_model)))
print(paste("Model Mean Absolute Error:",Mean_abs_error))
Mobile_Online_model_fit[1, "Mean_abs_error"] <- Mean_abs_error

# Bootstrap estimates of the coefficient standard errors
bootestim <- boot(data = Mobile_on_modelData, statistic = boot.fn, 
                  R = 1000, responseVar = "Mobile_GG")
cat("model variables and coefficients:")
bootestim$t0
bootestim
cat("\n\n\n")

boot.ci(bootestim, index = 1, conf = 0.999)
boot.ci(bootestim, index = 2, conf = 0.999)


# fit model with LOOCV and use this to estimate out of sample prediction 
# error
I <- !is.na(datas$Mobile_GG) & !is.na(datas$FB_GG_age_25_29) 

Mobile_on_modelData = data.frame(Mobile_GG = datas$Mobile_GG[I],
                                 FB_GG_age_25_29 = datas$FB_GG_age_25_29[I])

pred <- predictLOOCV(dataset = Mobile_on_modelData, response = "Mobile_GG",
                     indicators = "")
print("The SMAPE:")
error <- abs(Mobile_on_modelData$Mobile_GG - pred)
error <- error/((abs(Mobile_on_modelData$Mobile_GG)+abs(pred))/2)
error <- sum(error)/length(pred)
print(error)
Mobile_Online_model_fit[1, "smape"] <- error

write.csv(Mobile_Online_model_coefs, "../data/Mobile_Online_model_coefs.csv" )
write.csv(Mobile_Online_model_fit, "../data/Mobile_Online_model_fit.csv" )

fits <- rbind(fits, Mobile_Online_model_fit)

write.csv(fits, "../data/fit.csv" )



##                  The Mobile Online-Offline Regression Model
## ------------------------------------------------------------------------- ##
# This model's variables are chosen using a greedy-stepwise-forward 
# approach.
# --- Parameters
baseModel <- data.frame(datas$Mobile_GG)
names(baseModel) <- c("Mobile_GG")
varyName <- "Mobile_GG"
candidVars_names <- c(devIndicators,
                      GGIndicators,
                      mobileVars[mobileVars != "Is_South_Asia"],
                      FBAgeGG[FBAgeGG =="FB_GG_age_25_29"],
                      FBDeviceGG)
candidVars <- datas[,is.element(colnames(datas),candidVars_names)]
minCountries <- 20

# fit the model
finModel_variables <- build_model_with_greedy_stepwise_forward(
  baseModel, varyName, candidVars, minCountries)
Mobile_Online_Offline_model <- lm(Mobile_GG ~., finModel_variables)
print("The Mobile GG Online Offline Model")
summary(Mobile_Online_Offline_model)
Mobile_Online_Offline_model_coefs <- tidy(Mobile_Online_Offline_model)
Mobile_Online_Offline_model_fit <- glance(Mobile_Online_Offline_model)
Mobile_Online_Offline_model_fit[1, "Model"] <- "Mobile_Online_Offline_model"

Mean_abs_error <- mean(abs(residuals(Mobile_Online_Offline_model)))
print(paste("Model Mean Absolute Error:",Mean_abs_error))
Mobile_Online_Offline_model_fit[1, "Mean_abs_error"] <- Mean_abs_error

# Refit above model with standardized values for easier interpretation of
# model coefficients.
I <- !is.na(datas$Mobile_GG) &
  !is.na(datas$FB_GG_age_25_29) &
  !is.na(datas$low_unique_subscriber_penetration) &
  !is.na(datas$FB_iOS_device_users_GG) 

Mobile_onoff_modelData <- data.frame(Mobile_GG = datas$Mobile_GG[I],
                                     FB_GG_age_25_29 = scale(datas$FB_GG_age_25_29[I]),
                                     low_unique_subscriber_penetration = datas$low_unique_subscriber_penetration[I],
                                     FB_iOS_device_users_GG = scale(datas$FB_iOS_device_users_GG[I]))

Mobile_Online_Offline_model <- lm(Mobile_GG ~., Mobile_onoff_modelData)
print("The Mobile GG Online Offline Model")
summary(Mobile_Online_Offline_model)
# TODO

# Bootstrap estimates of the coefficient standard errors
cat("\n\n Bootstrap Estimates of regression coefficient standard errors:\n")
bootestim <- boot(data = Mobile_onoff_modelData, statistic = boot.fn, 
                  R = 1000, responseVar = "Mobile_GG")
cat("model variables and coefficients:")
bootestim$t0
bootestim
cat("\n\n\n")

boot.ci(bootestim, index = 1, conf = 0.999)
boot.ci(bootestim, index = 2, conf = 0.99)
boot.ci(bootestim, index = 3, conf = 0.95)
boot.ci(bootestim, index = 4, conf = 0.95)


# Use LOOCV to fit model and make predictions as a way to estimate
# out of sample prediction accuracy
I <- !is.na(datas$Mobile_GG) &
  !is.na(datas$FB_GG_age_25_29) &
  !is.na(datas$low_unique_subscriber_penetration) &
  !is.na(datas$FB_iOS_device_users_GG) 

Mobile_onoff_modelData <- data.frame(Mobile_GG = datas$Mobile_GG[I],
                                     FB_GG_age_25_29 = datas$FB_GG_age_25_29[I],
                                     low_unique_subscriber_penetration = datas$low_unique_subscriber_penetration[I],
                                     FB_iOS_device_users_GG = datas$FB_iOS_device_users_GG[I])

pred <- predictLOOCV(dataset = Mobile_onoff_modelData, response = "Mobile_GG",
                     indicators = "low_unique_subscriber_penetration")
print("The SMAPE:")
error <- abs(Mobile_onoff_modelData$Mobile_GG - pred)
error <- error/((abs(Mobile_onoff_modelData$Mobile_GG)+abs(pred))/2)
error <- sum(error)/length(pred)
print(error)
Mobile_Online_Offline_model_fit[1, "smape"] <- error

write.csv(Mobile_Online_Offline_model_coefs, "../data/Mobile_Online_Offline_model_coefs.csv" )
write.csv(Mobile_Online_Offline_model_fit, "../data/Mobile_Online_Offline_model_fit.csv" )

fits <- rbind(fits, Mobile_Online_Offline_model_fit)

write.csv(fits, "../data/fit.csv" )

# However, given the small amount of data the above model may be having too many
# variables and potentially overfitting. So here we try to us Principle Component
# Analysis for reducing the dimensionality of the model
Idx <- !is.element(colnames(Mobile_onoff_modelData), c("Mobile_GG"))
model.pca <- prcomp(x = Mobile_onoff_modelData[,Idx], retx = TRUE, scale = TRUE)

# model on just the first PC
XX <- data.frame(Mobile_GG = Mobile_onoff_modelData$Mobile_GG,
                 PCA.1 = model.pca$x[,1])
model.lm <- lm(Mobile_GG ~ ., XX)
summary(model.lm)
Mobile_Online_Offline_model_coefs2 <- tidy(model.lm)
Mobile_Online_Offline_model_fit2 <- glance(model.lm)
Mobile_Online_Offline_model_fit2[1, "Model"] <- "Mobile_Online_Offline_model2"

Mean_abs_error <- mean(abs(residuals(model.lm)))
print(paste("Model Mean Absolute Error:",Mean_abs_error))
Mobile_Online_Offline_model_fit2[1, "Mean_abs_error"] <- Mean_abs_error

pred <- predict(model.lm)
mean(abs(Mobile_onoff_modelData$Mobile_GG - pred))

# model on the first two PCs
XX <- data.frame(Mobile_GG = Mobile_onoff_modelData$Mobile_GG,
                 PCA.1 = model.pca$x[,1],
                 PCA.2 = model.pca$x[,2])
model.lm <- lm(Mobile_GG ~ ., XX)
summary(model.lm)
Mobile_Online_Offline_model_coefs3 <- tidy(model.lm)
Mobile_Online_Offline_model_fit3 <- glance(model.lm)
Mobile_Online_Offline_model_fit3[1, "Model"] <- "Mobile_Online_Offline_model3"

Mean_abs_error <- mean(abs(residuals(model.lm)))
print(paste("Model Mean Absolute Error:",Mean_abs_error))
Mobile_Online_Offline_model_fit3[1, "Mean_abs_error"] <- Mean_abs_error

mean(abs(predict(model.lm) - Mobile_onoff_modelData$Mobile_GG))

# Use LOOCV to get an estimate of out of sample prediction error for the 
# model with the first two PCs.
pred <- c() #Vector of predictions to be made
for (i in 1:nrow(Mobile_onoff_modelData)) {
  train.data <- Mobile_onoff_modelData[-i,]
  test.data <- Mobile_onoff_modelData[i,]
  
  # fit the PCA model on the training set and predict on validation set
  Idx <- !is.element(colnames(train.data), c("Mobile_GG"))
  model.pca <- prcomp(x = train.data[,Idx], retx = TRUE, scale = TRUE)
  
  XX <- data.frame(Mobile_GG = train.data$Mobile_GG,
                   PCA.1 = model.pca$x[,1],
                   PCA.2 = model.pca$x[,2])
  model.lm <- lm(Mobile_GG ~ ., XX)
  
  Idx <- !is.element(colnames(test.data),"Mobile_GG")
  test.data[,Idx] <- scale(test.data[,Idx], center = model.pca$center,
                           scale = model.pca$scale)
  test.data$PCA.1 <- as.numeric(as.matrix(test.data[,c(2:4)]) %*% model.pca$rotation[,1]) # first PCA
  test.data$PCA.2 <- as.numeric(as.matrix(test.data[,c(2:4)]) %*% model.pca$rotation[,2])
  pred <- c(pred, predict(model.lm, newdata = test.data))
  
}

cat("Results of using LOOCV for the model using PCA")
print("The SMAPE:")
error <- abs(Mobile_onoff_modelData$Mobile_GG - pred)
error <- error/((abs(Mobile_onoff_modelData$Mobile_GG)+abs(pred))/2)
error <- sum(error)/length(pred)
print(error)
Mobile_Online_Offline_model_fit2[1, "smape"] <- "NA"
Mobile_Online_Offline_model_fit3[1, "smape"] <- error

write.csv(Mobile_Online_Offline_model_coefs2, "../data/Mobile_Online_Offline_model_coefs2.csv" )
write.csv(Mobile_Online_Offline_model_fit2, "../data/Mobile_Online_Offline_model_fit2.csv" )
write.csv(Mobile_Online_Offline_model_coefs3, "../data/Mobile_Online_Offline_model_coefs3.csv" )
write.csv(Mobile_Online_Offline_model_fit3, "../data/Mobile_Online_Offline_model_fit3.csv" )

fits <- rbind(fits, Mobile_Online_Offline_model_fit2)
fits <- rbind(fits, Mobile_Online_Offline_model_fit3)

write.csv(fits, "../data/fit.csv" )

# Bootstrap standard errors with the PCA model
cat("\n\n Bootstrap Estimates of regression coefficient standard errors:\n")
bootestim <- boot(data = XX[,c("Mobile_GG","PCA.1")], statistic = boot.fn, 
                  R = 1000, responseVar = "Mobile_GG")
cat("model variables and coefficients:")
bootestim$t0
bootestim
cat("\n\n\n")

boot.ci(bootestim, index = 1, conf = 0.999)
boot.ci(bootestim, index = 2, conf = 0.999)




##                  The Mobile Offline Regression Model
## ------------------------------------------------------------------------- ##
# this model appears in the report by the GSMA on mobile gender gaps
# the model is fit with standardized variables for ease of interpretation of
# the coefficients
I <- !is.na(datas$Mobile_GG) &
  !is.na(datas$Multidimensional_poverty_index) &
  !is.na(datas$low_unique_subscriber_penetration) &
  !is.na(datas$Is_South_Asia)

Mobile_off_modelData <- data.frame(Mobile_GG = datas$Mobile_GG[I],
                                   Multidimensional_poverty_index = scale(datas$Multidimensional_poverty_index[I]),
                                   low_unique_subscriber_penetration = datas$low_unique_subscriber_penetration[I],
                                   Is_South_Asia = datas$Is_South_Asia[I])

Mobile_Offline_model <- lm(Mobile_GG ~ Multidimensional_poverty_index +
                             low_unique_subscriber_penetration +
                             Is_South_Asia, Mobile_off_modelData)
print("The Mobile GG Offline Model")
summary(Mobile_Offline_model)
Mobile_Offline_model_coefs <- tidy(Mobile_Offline_model)
Mobile_Offline_model_fit <- glance(Mobile_Offline_model)
Mobile_Offline_model_fit[1, "Model"] <- "Mobile_Offline_model"

Mean_abs_error <- mean(abs(residuals(Mobile_Offline_model)))
print(paste("Model Mean Absolute Error:",Mean_abs_error))
Mobile_Offline_model_fit[1, "Mean_abs_error"] <- Mean_abs_error

# Bootstrap estimates of the coefficient standard errors
cat("\n\n Bootstrap Estimates of regression coefficient standard errors:\n")
bootestim <- boot(data = Mobile_off_modelData, statistic = boot.fn, 
                  R = 1000, responseVar = "Mobile_GG")
cat("model variables and coefficients:")
bootestim$t0
bootestim
cat("\n\n\n")

boot.ci(bootestim, index = 1, conf = 0.999)
boot.ci(bootestim, index = 2, conf = 0.95)
boot.ci(bootestim, index = 3, conf = 0.95)
boot.ci(bootestim, index = 4, conf = 0.95)


# Use LOOCV to make predictions so as to estimate out of sample prediction error
I <- !is.na(datas$Mobile_GG) &
  !is.na(datas$Multidimensional_poverty_index) &
  !is.na(datas$low_unique_subscriber_penetration) &
  !is.na(datas$Is_South_Asia)

Mobile_off_modelData <- data.frame(Mobile_GG = datas$Mobile_GG[I],
                                   Multidimensional_poverty_index = datas$Multidimensional_poverty_index[I],
                                   low_unique_subscriber_penetration = datas$low_unique_subscriber_penetration[I],
                                   Is_South_Asia = datas$Is_South_Asia[I])

Mobile_Offline_model <- lm(Mobile_GG ~ Multidimensional_poverty_index +
                             low_unique_subscriber_penetration +
                             Is_South_Asia, Mobile_off_modelData)

pred <- predictLOOCV(dataset = Mobile_off_modelData, response = "Mobile_GG",
                     indicators = c("low_unique_subscriber_penetration","Is_South_Asia"))
print("The SMAPE:")
error <- abs(Mobile_off_modelData$Mobile_GG - pred)
error <- error/((abs(Mobile_off_modelData$Mobile_GG)+abs(pred))/2)
error <- sum(error)/length(pred)
print(error)
Mobile_Offline_model_fit[1, "smape"] <- error

write.csv(Mobile_Offline_model_coefs, "../data/Mobile_Offline_model_coefs.csv" )
write.csv(Mobile_Offline_model_fit, "../data/Mobile_Offline_model_fit.csv" )

fits <- rbind(fits, Mobile_Offline_model_fit)

write.csv(fits, "../data/fits.csv" )



#further_analysis_for_paper <-
#  function(baseModel, varyName, candidVars, minCountries = 0) {
## ------------------------------------------------------------------------- ##
##   Improving Model performance using Correction Factors
## ------------------------------------------------------------------------- ##
# For the Online models performance can be further improved if one corrects the
# FB gender gap index for potential biases. Here is some analysis on using 
# a correction factor to debias the FB gender gap index to improve the 
# performance of the Online model. 

# generate a plot of the Internet GG versus the FB GG 
# coloured by the Literacy GG of each country. This provides intuition for
# how to debias the FB GG.
x <- datas$FB_GG_age_18_plus
xlab <- "FB GGI"
xlim <- c(0.3,1)
y <- datas$Internet_GG
ylab <- "Ground Truth Internet GGI"
ylim <- c(0.5,1)
#c <- datas$Int_pentr_IWS
c <- datas$Literacy_GG
sc <- summary(c)
#colour <- c("orange","red","purple","green")
colour <- rep("black",4)

make_coloured_xy_plot(x, xlab, xlim, y, ylab, ylim, c, sc, colour) 


# In this part the Literacy GG is used to debias the FB GG in order to
# further improve the performance of the Online model. The correction 
# factor used is of the form (1 + k*(1-c)) where c is the variable
# being used for debiasing (literacy GG in this case). (c is in [0,1])
# One can repeat this analysis for different choices of correction factors
x <- datas$FB_GG_age_18_plus
y <- datas$Internet_GG
c <- datas$Literacy_GG
k <- seq(-10,10,0.1)
corrResults <- regress_with_correction_factors(x, y, c, k)

plot(corrResults$k, corrResults$adjR_squared, 
     xlab="Tuning parameter",ylab="Adjusted R-squared of regression", cex=0.4)

bestk <- corrResults$k[which.max(corrResults$adjR_squared)]
bestk
bestAdjRsqr <- max(corrResults$adjR_squared)
bestAdjRsqr
summary(lm(y ~ x))$adj.r.squared # compare with Adjusted R-squred with no bias correction





## ------------------------------------------------------------------------- ##
## Filling in the Data gaps -- World Maps of Model Predictions & Ground Truth
## ------------------------------------------------------------------------- ##
# In this section we generate high resolution maps of the ground truth 
# gender gaps as well as the predictions we have made using our online models. 
# This is done for both the Internet and Mobile Phone gender gaps.

## Model predictions
# save predictions from all the models that were fit before so they can 
# be plotted on maps
# predictions are capped at 1

# Internet Online Model
Internet_on_modelData <- data.frame(Internet_GG = datas$Internet_GG, 
                                    FB_GG_age_18_plus = datas$FB_GG_age_18_plus)
Internet_online_model <- lm(Internet_GG ~ FB_GG_age_18_plus, Internet_on_modelData)
Internet_online_model_prediction <- predict(Internet_online_model,datas)
I <- !is.na(Internet_online_model_prediction) & Internet_online_model_prediction > 1
Internet_online_model_prediction[I] <- 1

# Internet Online-Offline Model
Internet_onoff_modelData <- data.frame(Internet_GG = datas$Internet_GG,
                                       FB_GG_age_18_plus = datas$FB_GG_age_18_plus,
                                       log_GDP_capita = datas$log_GDP_capita,
                                       Literacy_GG = datas$Literacy_GG,
                                       Educ_attn_GG_subindex = datas$Educ_attn_GG_subindex)
Internet_Online_Offline_model <- lm(Internet_GG ~., Internet_onoff_modelData)
Internet_Online_Offline_model_prediction <- predict(Internet_Online_Offline_model,datas)
I <- !is.na(Internet_Online_Offline_model_prediction) & 
  Internet_Online_Offline_model_prediction > 1
Internet_Online_Offline_model_prediction[I] <- 1

# Internet Offline Model
Internet_off_modelData <- data.frame(Internet_GG = datas$Internet_GG,
                                     Int_pentr_IWS = datas$Int_pentr_IWS,
                                     Enrol_ter_educ_GG = datas$Enrol_ter_educ_GG,
                                     Econ_opp_GG_subindex = datas$Econ_opp_GG_subindex,
                                     GGG_score = datas$GGG_score)
Internet_Offline_model <- lm(Internet_GG ~., Internet_off_modelData)
Internet_Offline_model_prediction <- predict(Internet_Offline_model,datas)
I <- !is.na(Internet_Offline_model_prediction) & Internet_Offline_model_prediction > 1
Internet_Offline_model_prediction[I] <- 1

# Mobile Online Model
Mobile_on_modelData = data.frame(Mobile_GG = datas$Mobile_GG,
                                 FB_GG_age_25_29 = datas$FB_GG_age_25_29)
Mobile_Online_model <- lm(Mobile_GG ~ FB_GG_age_25_29,Mobile_on_modelData)
Mobile_Online_model_prediction <- predict(Mobile_Online_model, datas)
I <- !is.na(Mobile_Online_model_prediction) & Mobile_Online_model_prediction > 1
Mobile_Online_model_prediction[I] <- 1

# Mobile Online-Offline Model
Mobile_onoff_modelData <- data.frame(Mobile_GG = datas$Mobile_GG,
                                     FB_GG_age_25_29 = datas$FB_GG_age_25_29,
                                     low_unique_subscriber_penetration = datas$low_unique_subscriber_penetration,
                                     FB_iOS_device_users_GG = datas$FB_iOS_device_users_GG)
Mobile_Online_Offline_model <- lm(Mobile_GG ~., Mobile_onoff_modelData)
Mobile_Online_Offline_model_prediction <- predict(Mobile_Online_Offline_model, datas)
I <- !is.na(Mobile_Online_Offline_model_prediction) & 
  Mobile_Online_Offline_model_prediction > 1
Mobile_Online_Offline_model_prediction[I] <- 1

# Mobile Offline Model
Mobile_off_modelData <- data.frame(Mobile_GG = datas$Mobile_GG,
                                   Multidimensional_poverty_index = datas$Multidimensional_poverty_index,
                                   low_unique_subscriber_penetration = datas$low_unique_subscriber_penetration,
                                   Is_South_Asia = datas$Is_South_Asia)
Mobile_Offline_model <- lm(Mobile_GG ~ Multidimensional_poverty_index +
                             low_unique_subscriber_penetration +
                             Is_South_Asia, Mobile_off_modelData)
Mobile_Offline_model_prediction <- predict(Mobile_Offline_model, datas)
I <- !is.na(Mobile_Offline_model_prediction) & Mobile_Offline_model_prediction > 1
Mobile_Offline_model_prediction[I] <- 1



##            Plot predictions for Internet Gender Gaps
## ------------------------------------------------------------------------- ##

# Plot above predictions on a map
# ---------------------------- Map Plot parameters --------------------------- #
# The data to be plotted
df <- data.frame(datas$ISO3Code,datas$Country,
                 datas$Internet_GG,
                 Internet_online_model_prediction)
names(df) <- c("ISO3Code","Country",
               "Internet_GG","Internet_online_model_prediction")

# countries for which to make prediction
crit <- datas$FB_18_plus_pop_2017 > 200000 & datas$Country != "China"

# Intervals and their colours
cutVector <- c(0.5,0.7,0.76,0.82,0.88,0.94,1)
colourVector <- c("#FFE0B3FF", "#FFFF00FF", "#E6FF00FF", "#00FF4DFF",
                  "#00E5FFFF", "#4C00FFFF")

# ------------------------------- Plot the Maps ------------------------------ #

# The Ground Truth Internet GG
sPDF <- joinCountryData2Map(ITU_dataset, joinCode="ISO3", nameJoinColumn="ISO3Code",
                            nameCountryColumn="Country", 
                            suggestForFailedCodes=TRUE,
                            verbose=TRUE)
mapDevice()
mapParams <- mapCountryData(mapToPlot=sPDF, 
                            nameColumnToPlot="Internet_GG",
                            colourPalette=colourVector, 
                            catMethod=cutVector,
                            numCats=length(cutVector),
                            mapTitle="",
                            missingCountryCol="grey",
                            addLegend=FALSE)
do.call(addMapLegend,c(mapParams,legendLabels="all",legendIntervals="data"))

# The Internet GG predicted by the online Model
sPDF <- joinCountryData2Map(df[crit,], joinCode="ISO3", nameJoinColumn="ISO3Code",
                            nameCountryColumn="Country", 
                            suggestForFailedCodes=TRUE,
                            verbose=TRUE)
mapDevice()
mapParams <- mapCountryData(mapToPlot=sPDF, 
                            nameColumnToPlot="Internet_online_model_prediction",
                            colourPalette=colourVector, 
                            catMethod=cutVector,
                            numCats=length(cutVector),
                            mapTitle="",
                            missingCountryCol="grey",
                            addLegend=FALSE)
do.call(addMapLegend,c(mapParams,legendLabels="all",legendIntervals="data"))



##            Plot predictions for Mobile Gender Gaps
## ------------------------------------------------------------------------- ##

# Plot above predictions on a map
# ---------------------------- Map Plot parameters --------------------------- #
# The data to be plotted
df <- data.frame(datas$ISO3Code,datas$Country,
                 datas$Mobile_GG,
                 Mobile_Online_model_prediction)
names(df) <- c("ISO3Code","Country",
               "Mobile_GG","Mobile_Online_model_prediction")

# countries for which to make prediction
crit <- datas$FB_18_plus_pop_2017 > 200000 & datas$Country != "China"

# Intervals and their colours
cutVector <- c(0.48,0.7,0.76,0.82,0.88,0.94,1)
colourVector <- c("#FFE0B3FF", "#FFFF00FF", "#E6FF00FF", "#00FF4DFF",
                  "#00E5FFFF", "#4C00FFFF")

# ------------------------------- Plot the Maps ------------------------------ #

# The Ground Truth Mobile GG
sPDF <- joinCountryData2Map(df, joinCode="ISO3", nameJoinColumn="ISO3Code",
                            nameCountryColumn="Country", 
                            suggestForFailedCodes=TRUE,
                            verbose=TRUE)
mapDevice()
mapParams <- mapCountryData(mapToPlot=sPDF, 
                            nameColumnToPlot="Mobile_GG",
                            colourPalette=colourVector, 
                            catMethod=cutVector,
                            numCats=length(cutVector),
                            mapTitle="",
                            missingCountryCol="grey",
                            addLegend=FALSE)
do.call(addMapLegend,c(mapParams,legendLabels="all",legendIntervals="data"))

# The Mobile GG predicted by the online Model
sPDF <- joinCountryData2Map(df[crit,], joinCode="ISO3", nameJoinColumn="ISO3Code",
                            nameCountryColumn="Country", 
                            suggestForFailedCodes=TRUE,
                            verbose=TRUE)
mapDevice()
mapParams <- mapCountryData(mapToPlot=sPDF, 
                            nameColumnToPlot="Mobile_Online_model_prediction",
                            colourPalette=colourVector, 
                            catMethod=cutVector,
                            numCats=length(cutVector),
                            mapTitle="",
                            missingCountryCol="grey",
                            addLegend=FALSE)
do.call(addMapLegend,c(mapParams,legendLabels="all",legendIntervals="data"))




## ------------------------------------------------------------------------- ##
##    Tables for the appendix of the paper.
## ------------------------------------------------------------------------- ##
# In this section we store the results of the predictions of the three 
# Internet GG models and the three mobile GG models for all countries 
# as well as the ground truth values for these.

# Tabele of Model predictions and the ground truth
## ------------------------------------------------------------------------- ##
digits <- 3 # number of significant digits for data.
Appendix_table <- data.frame(datas$Country,
                             datas$ISO3Code,
                             signif(datas$Internet_GG,digits),
                             signif(Internet_online_model_prediction,digits),
                             signif(Internet_Online_Offline_model_prediction,digits),
                             signif(Internet_Offline_model_prediction,digits),
                             
                             signif(datas$Mobile_GG,digits),
                             signif(Mobile_Online_model_prediction,digits),
                             signif(Mobile_Online_Offline_model_prediction,digits),
                             signif(Mobile_Offline_model_prediction,digits))

colnames(Appendix_table) <- c("Country",
                              "ISO3Code",
                              "Ground Truth Internet GG",
                              "Internet online model prediction",
                              "Internet Online-Offline model prediction",
                              "Internet Offline model prediction",
                              
                              "Mobile_GG",
                              "Mobile Online model prediction",
                              "Mobile Online-Offline model prediction",
                              "Mobile Offline model prediction")

write.csv(Appendix_table, "../data/Appendix_table_model_predictions.csv")

further_analysis_for_paper <-
  function(baseModel, varyName, candidVars, minCountries = 0) {
# Table of number of countries can predict for broken down by Development/Income
## ------------------------------------------------------------------------- ##
# breakdown for all countries in dataset
summary(datas$UN_M49_Class)
summary(datas$WB_Income_Class)

# breakdown for countried with ground truth ITU data
I <- !is.na(ITU_dataset$Internet_GG) 
summary(ITU_dataset$UN_M49_Class[I])
summary(ITU_dataset$WB_Income_Class[I])

# breakdown for countries for which we predict with the 
# Internet Gender Gap with the Online Model
I <- !is.na(Internet_online_model_prediction) & 
      datas$FB_18_plus_pop_2017 > 200000 & 
      datas$Country != "China"
summary(datas$UN_M49_Class[I])
summary(datas$WB_Income_Class[I])

# breakdown for countried with ground truth GSMA data
I <- !is.na(datas$Mobile_GG) 
summary(datas$UN_M49_Class[I])
summary(datas$WB_Income_Class[I])

# breakdown for countries for which we predict 
# Mobile Phone Gender Gap with the Online Model
I <- !is.na(Mobile_Online_model_prediction) & 
      datas$FB_18_plus_pop_2017 > 200000 & 
      datas$Country != "China"
summary(datas$UN_M49_Class[I])
summary(datas$WB_Income_Class[I])









## ------------------------------------------------------------------------- ##
## Some Further Analysis on the Greedy step-wise forward model selection
## ------------------------------------------------------------------------- ##
# This analysis was done based on the comments made by the reviewers after 
# the first review of the paper. Here we explore different possible models
# that could have been obtained for all the models that we developed using
# step-wise forward greedy methods; we do this by using a cross validation
# appraoch with the step-wise forward algorithm to see the variety of models
# that could have been obtained. We consider variations of the algorithm
# depending on whether variable significance is taken into account when 
# picking variables.



##                  Internet Online-Offline Model
## ------------------------------------------------------------------------- ##
# setup
candidVars <-  c(devIndicators,GGIndicators,"FB_GG_age_18_plus")
I <- rep(TRUE, nrow(datas)) 
numDatas <- rep(0, length(candidVars))
for (i in 1:length(candidVars)) {
  I <- I & !is.na(datas[,candidVars[i]])
  numDatas[i] <- sum(!is.na(datas$Internet_GG) & !is.na(datas[,candidVars[i]]))
}
sum(I)

candidVars <- candidVars[numDatas >= 70]

nfold <- 2 # how many fold cross validation. the larger this is the larger the number of models
cat("\n\n====================================================================\n")
cat(nfold,"partitions of the data with variable pvalue based on parametric OLS assumptions\n\n")
set.seed(131314)
I <- !is.na(datas$Internet_GG)
IntOnOffmodelingResultsParam <- build_model_with_greedy_stepwise_forward_2(data = datas[I,], 
                                                                         response = "Internet_GG", varSig = "parametric", 
                                                                         pval = 0.05, 
                                                                         candidVars = candidVars, 
                                                                         nfold = nfold, 
                                                                         errorMetric = "MSE", minCountries=0)
IntOnOffmodelingResultsParam

cat("\n\n====================================================================\n")
cat(nfold,"partitions of the data with variable pvalue based on bootstrap\n\n")
set.seed(131314)
I <- !is.na(datas$Internet_GG)
IntOnOffmodelingResultsBoot <- build_model_with_greedy_stepwise_forward_2(data = datas[I,], 
                                                                        response = "Internet_GG", varSig = "bootstrap", 
                                                                        pval = 0.05, 
                                                                        candidVars = candidVars, 
                                                                        nfold = nfold, 
                                                                        errorMetric = "MSE", minCountries=0)
IntOnOffmodelingResultsBoot

cat("\n\n====================================================================\n")
cat(nfold,"partitions of the data with no variable significance\n\n")
set.seed(131314)
I <- !is.na(datas$Internet_GG)
IntOnOffmodelingResults <- build_model_with_greedy_stepwise_forward_2(data = datas[I,], 
                                                                    response = "Internet_GG", varSig = "none", 
                                                                    pval = 0.05, 
                                                                    candidVars = candidVars, 
                                                                    nfold = nfold, 
                                                                    errorMetric = "MSE", minCountries=0)
IntOnOffmodelingResults




##                  Internet Offline Model
## ------------------------------------------------------------------------- ##
# setup
Idx <- !is.element(devIndicators,c("FB_Internet_penetration_ratio","FB_penentration"))
candidVars <-  c(devIndicators[Idx],GGIndicators)
I <- rep(TRUE, nrow(datas)) 
numDatas <- rep(0, length(candidVars))
for (i in 1:length(candidVars)) {
  I <- I & !is.na(datas[,candidVars[i]])
  numDatas[i] <- sum(!is.na(datas$Internet_GG) & !is.na(datas[,candidVars[i]]))
}
sum(I)

candidVars <- candidVars[numDatas >= 70]
I <- !is.na(datas$Internet_GG)
for (i in 1:length(candidVars)) {I <- I & !is.na(datas[,candidVars[i]])}
sum(I)

nfold <- 2 # how many fold cross validation. the larger this is the larger the number of models
cat("\n\n====================================================================\n")
cat(nfold,"partitions of the data with variable pvalue based on parametric assumptions\n\n")
set.seed(131314)
set.seed(33452345)
I <- !is.na(datas$Internet_GG)
IntOffmodelingResults5foldParam <- build_model_with_greedy_stepwise_forward_2(data = datas[I,], 
                                                                            response = "Internet_GG", varSig = "parametric", 
                                                                            pval = 0.05, 
                                                                            candidVars = candidVars, 
                                                                            nfold = nfold, 
                                                                            errorMetric = "MSE", minCountries=0)
IntOffmodelingResults5foldParam

cat("\n\n====================================================================\n")
cat(nfold,"partitions of the data with variable pvalue based on bootstrap\n\n")
set.seed(131314)
I <- !is.na(datas$Internet_GG)
IntOffmodelingResults5foldBoot <- build_model_with_greedy_stepwise_forward_2(data = datas[I,], 
                                                                           response = "Internet_GG", varSig = "bootstrap", 
                                                                           pval = 0.05, 
                                                                           candidVars = candidVars, 
                                                                           nfold = nfold, 
                                                                           errorMetric = "MSE", minCountries=0)
IntOffmodelingResults5foldBoot

cat("\n\n====================================================================\n")
cat(nfold,"partitions of the data with no significance level requirements\n\n")
set.seed(131314)
I <- !is.na(datas$Internet_GG)
IntOffmodelingResults <- build_model_with_greedy_stepwise_forward_2(data = datas[I,], 
                                                                  response = "Internet_GG", varSig = "none", 
                                                                  pval = 0.05, 
                                                                  candidVars = candidVars, 
                                                                  nfold = nfold, 
                                                                  errorMetric = "MSE", minCountries=0)
IntOffmodelingResults


##                  Mobile Online-Offline Model
## ------------------------------------------------------------------------- ##
# setup
candidVars <- c(devIndicators,
                GGIndicators,
                mobileVars[mobileVars != "Is_South_Asia"],
                FBAgeGG[FBAgeGG =="FB_GG_age_25_29"],
                FBDeviceGG)
I <- rep(TRUE, nrow(datas))
numDatas <- rep(0, length(candidVars))
for (i in 1:length(candidVars)) {
  I <- I & !is.na(datas[,candidVars[i]])
  numDatas[i] <- sum(!is.na(datas$Mobile_GG) & !is.na(datas[,candidVars[i]]))
}
sum(I)

candidVars <- candidVars[numDatas >= 19]

nfold <- 2 # how many fold cross validation. the larger this is the larger the number of models

# using the other algorithm:
cat("5 Fold Cross Validation \n ------------------------------------------------\n")
cat("\n\n====================================================================\n")
cat(nfold, "partitions of the data with variable pvalue based on parametric OLS assumptions\n\n")
set.seed(131314)
I <- !is.na(datas$Mobile_GG)
MobOnOffstepWiseResults5foldParam <- build_model_with_greedy_stepwise_forward_2(data = datas[I,],
                                                                              response = "Mobile_GG", varSig = "parametric",
                                                                              pval = 0.05,
                                                                              candidVars = candidVars,
                                                                              nfold = nfold,
                                                                              errorMetric = "MSE", minCountries=20)
MobOnOffstepWiseResults5foldParam
# 
cat("\n\n====================================================================\n")
cat(nfold,"partitions of the data with variable pvalue based on bootstrap\n\n")
set.seed(131314)
I <- !is.na(datas$Mobile_GG)
MobOnOffstepWiseResults5foldBoot <- build_model_with_greedy_stepwise_forward_2(data = datas[I,],
                                                                             response = "Mobile_GG", varSig = "bootstrap",
                                                                             pval = 0.05,
                                                                             candidVars = candidVars,
                                                                             nfold = nfold,
                                                                             errorMetric = "MSE", minCountries=20)

MobOnOffstepWiseResults5foldBoot


# using the other algorithm:
cat("5 Fold Cross Validation \n ------------------------------------------------\n")
cat("\n\n====================================================================\n")
cat(nfold, "partitions of the data with variable no p-value\n\n")
set.seed(131314)
I <- !is.na(datas$Mobile_GG)
MobOnOffstepWiseResults <- build_model_with_greedy_stepwise_forward_2(data = datas[I,],
                                                                    response = "Mobile_GG", varSig = "none",
                                                                    pval = 0.05,
                                                                    candidVars = candidVars,
                                                                    nfold = nfold,
                                                                    errorMetric = "MSE", minCountries=20)
MobOnOffstepWiseResults

}

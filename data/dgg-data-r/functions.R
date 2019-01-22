## ------------------------------------------------------------------------- ##
## title: functions
## Descriptions: The code here contains custom made functions that were used
##      to perform some of the data analysis tasks. Descriptions of each 
##      function and what it does is given.
## Date: Updated 4 Feb 2018
## ------------------------------------------------------------------------- ##



# ============================================================================= #
#
# This function takes as input:
# groundTruth <- this is the name of a column in dataset containing the data 
#     for the ground truth variable.
# variables <- This is a vector containing the names of the columns of dataset
#     which contain data on the variables whose correlations with the
#     groundTruth are to be computed
# variablesNames <- a human-readable name for the variables. This more intuitive
#     name is used to store the results.
# dataset <- a dataframe containing data for all the variables whose names 
#     have been stored in the arguments "variables" & "groundTruth" defined above
#
# This Function computes the correlation of each of the variables with the 
# groundTruth as well as the number of data points used for computing the
# correlation. It returns a dataframe each row of which stores the data for 
# each one of the "variables" with two columns, one for the correlation
# and another for the number of countries. Each row is named after the 
# variable in "variables" for which it contains data using the intuitive name
# given in the vector "variablesNames"
# 
# ============================================================================= #

compute_Correlations_with_ground_truth <-
  function(groundTruth, variables, VariablesNames, dataset) {
    
    # the dataframe to store the results
    Results <- data.frame(row.names = variablesNames)
    Results$Correlations <- rep(0,length(variables))
    Results$NumCountries <- rep(0,length(variables))
    
    # compute correlations of the groundTruth with the variables. 
    # Store the results in the dataframe.
    for (i in 1:length(variables)) {
      # for each of the variables compute and store:
      
      # its correlation with the ground truth 
      Results$Correlations[i] <- 
        cor(dataset[,names(dataset) == groundTruth],
            dataset[,names(dataset) == variables[i]],use="complete.obs")
      
      # number of countries used in computing the correlation
      I <- !is.na(dataset[,names(dataset) == groundTruth]) &
        !is.na(dataset[,names(dataset)==variables[i]])
      Results$NumCountries[i] <- sum(I)
    }
    
    return(Results)
  }


# ============================================================================= #
# 
# This function takes arguments:
# baseModel <- a data frame which contains the explanatory and response 
#       variables in the initial model
# varyName <- the name of the response variable in the regression 
#       (name of col. of baseModel)
# candidVars <- A dataframe of candidate variables to consider adding to 
#       the baseModel
# minCountries <- minimum number of data points on which the regression Models 
#       should be estimated. This ensures that variables with lots of missing
#       values which may result in good models fitted on a very small subset
#       of the data points are not considered. Default value is 0 meaning
#       there is no restriction on the number of data points.
# The function runs a greedy stepwise forward algorithm which iteratively goes 
# through the varibles in "candidVars" and adds the one that gives the highest 
# adjusted R-squared to the current model provided all variables, excepting 
# the intercept, in the resulting model are significant at the 5% p-value. 
# It keeps doing so until we run out of variables to try or until reaching 
# a model where none of the remaining variables can be added while 
# satisfying the above stated condition. At each step, the resulting model
# as well as the number of data points it was fitted on are printed.
# 
# It returns a dataframe "inModel" which contains the explanatory variable 
# and response variables in the final best model chosen by the algorithm.
# ============================================================================= #

build_model_with_greedy_stepwise_forward <-
  function(baseModel, varyName, candidVars, minCountries = 0) {
    # variables in the best model so far including the response variable
    inModel <- baseModel
    # rename response var for now
    names(inModel)[names(inModel)==varyName] <- "response_var" 
    
    # variables to consider including in the model
    candidVarsNames <- names(candidVars)
    
    cnt <- 0
    
    while (length(candidVarsNames) > 0) {
      # as long as there is a variable to try
      cnt <- cnt + 1
      
      # what is the best adjusted R-squared achieved so far
      bestAdjRsqr <- summary(lm(response_var ~ .,inModel))$adj.r.squared
      idx = 0 # index of variable to enter the model next
      bestCountryCnt <- 0
      
      for (i in 1:length(candidVarsNames)) {
        # for each candidate variable check the adj R squared 
        # if it is added to the model and keep track of the one 
        # that gives the highest adjusted r-squared while leaving
        # all model variables significant at the 5% level
        tempModel <- data.frame(inModel,candidVars[,i])
        names(tempModel)[length(names(tempModel))] <- candidVarsNames[i] 
        
        model <- lm(response_var ~., tempModel)
        adjRsqr <- summary(model)$adj.r.squared
        #rw <- names(summary(model)$coefficients[,4]) == candidVarsNames[i] 
        l <- length(summary(model)$coefficients[,4])
        variablePval <- summary(model)$coefficients[,4][2:l]
        
        # no. of estimated coefficients excluding intercept
        ncoeff <- length(names(tempModel)) - 1 
        
        # How many countries is this model based on?
        countryCnt <- rep(1,length(tempModel[,1]))
        for (j in 1:length(names(tempModel))) {
          countryCnt <- countryCnt & !is.na(tempModel[,j])
        }
        countryCnt <- sum(countryCnt)
        
        # keep track of the variable that satisfies the
        # conditions. 
        if (adjRsqr > bestAdjRsqr & 
            sum(variablePval < 0.05) == ncoeff & 
            countryCnt >= minCountries) {
          bestAdjRsqr <- adjRsqr
          idx = i
          bestCountryCnt <- countryCnt
        }
      }
      # if there is no variable to be added, stop
      if (idx == 0) {break}
      
      # else, update the current model by incorporating the 
      # next variable into the model
      n <- candidVarsNames
      inModel <- data.frame(inModel,candidVars[,idx])
      names(inModel)[length(names(inModel))] <- candidVarsNames[idx]
      
      if (idx == 1 & length(candidVarsNames) == 1) {
        candidvars <- c()
        candidVarsNames <- c()
      }
      else if (idx == 1) {
        candidVars <- candidVars[,(idx+1):length(n)]
        candidVarsNames <- candidVarsNames[(idx+1):length(n)]
        names(candidVars) <- candidVarsNames
        
      }
      else if (idx > 1 & idx < length(candidVarsNames)) {
        candidVars <- data.frame(candidVars[,1:(idx-1)],
                                 candidVars[,(idx+1):length(n)])
        candidVarsNames <- c(candidVarsNames[1:(idx-1)],
                             candidVarsNames[(idx+1):length(n)])
        names(candidVars) <- candidVarsNames
      }
      else {
        candidVars <- candidVars[,1:(idx-1)]
        candidVarsNames <- candidVarsNames[1:(idx-1)]
        names(candidVars) <- candidVarsNames
      }
      
      # print the results of this iteration
      print(paste("round",cnt,"results"))
      print(paste("Model fitted on",bestCountryCnt,"Countries"))
      print("------------------------------------------------------")
      print(summary(lm(response_var ~., inModel)))
      print("")
      print("### =============================================================== ###")
    }
    
    # Revert the name of the explanatory 
    # variable back to what it was
    names(inModel)[names(inModel)=="response_var"] <- varyName 
    
    return(inModel)
  }





# ============================================================================= #
# 
# This function takes arguments:
# data <- a dataframe containing the 'response' and 'candidVars'
# response <- the name of the response variable in the regression 
# varSig <- takes values c("none","parametric","bootstrap") indicating
#   whether the model should only keep variables that are significant or not
#   ("none"). If the model cares about variable significance then this should
#   be either based on "parametric" (based on OLS normality assumptions) or
#   "bootstrap" which is more robust to violations of normality.default = "none"
# pval <- p-value to test for significance if varSig == "parametric" (default == 0.05)
# candidVars <- A dataframe of candidate variables to consider in the model
# nfold <- number of folds for cross-validation (default is 10)
# errorMetric <- What error metric on the validation set to use for variable
#   selection. can take values c("MSE")
# minCountries <- minimum number of data points on which the regression Models 
#       should be estimated. This ensures that variables with lots of missing
#       values which may result in good models fitted on a very small subset
#       of the data points are not considered. Default value is 0 meaning
#       there is no restriction on the number of data points.
#
# The function runs a greedy stepwise forward algorithm just as the function 
# above (the one called "build_model_with_greedy_stepwise_forward"). 
# However, it splits the data into nfold pieces and uses a cross validation 
# approach to fit nfold different models each fit on (nfold - 1) of the 
# pieces with the variable selection happening based on the performance of
# the model on the validation set.
# 
# It returns a list whose first entry is a dataframe containing summary
# of the performance of the nfold different models. The second entry is
# a list of length nfold each entry of which is a character vector specifying 
# the names of the variables in that model.
# ============================================================================= #

build_model_with_greedy_stepwise_forward_2 <-
  function(data, response, varSig = "none", pval = 0.05, 
           candidVars, nfold = 10, errorMetric = "MSE", minCountries=0) {
    
    ## keep the full list of candidate variables
    FullCandidVars <- candidVars
    
    ### Create nfold split of the data for cross-validation
    N <- nrow(data)
    permute <- sample(1:N, N) # randomly permute the data
    data <- data[permute,]
    cat("Permutations of data: ", permute)
    
    foldIdx <- list() # list of vectors each keeping indices of one fold
    for (i in 1:nfold) {
      newfold <- seq(i,N,nfold)
      foldIdx <- c(foldIdx, list(newfold))
    }
    
    ### setup variables -- a dataframe to keep summary of models
    models <- data.frame(Id = 1:nfold, CV_MSE = 0, CV_SMAPE = 0, 
                         CV_Rsquared = 0, fullAdjRsquared = 0, numData = 0)
    
    modelVars <- list() # keep a list of model variables
    colnames(data)[colnames(data) == response] <- "responseVar" # for ease of reference
    
    ### Leave out one fold at a time and fit a model with 
    ### greedy-stepwise forward
    for (i in 1:nfold) {
      #for(i in 4) { 
      # reset the list of candidate variables to use in this model
      candidVars <- FullCandidVars
      cat("Fold = ", i, " model fitting\n ------------------------------------- \n")
      cat("countries in validation set: ", as.character(data$Country[foldIdx[[i]]],"\n\n"))
      
      train.data <- data[-foldIdx[[i]], ]
      valid.data <- data[foldIdx[[i]], ]
      cap <- nrow(train.data) - 1 # cap on number of variables to be less than the data
      # variables 
      inModel <- c("responseVar") # list of variables in the current model
      
      # performance of null model
      currPerformance <- mean((valid.data[,"responseVar"] - mean(train.data[,"responseVar"]))^2)
      
      ### Go through candidate variables and fit models in a stepwise process
      # & length(inModel) < (cap-1) # potential other condition 
      while(length(candidVars) > 0 & length(inModel) < cap) {
        
        cat("Current existing model variables: ", inModel, "\n")
        
        ### for this iteration make one round through all remaining variables
        ### and pick the one with lowest error on validation set.
        l <- length(candidVars)
        
        # save results for adding each candidate variable to current model
        error <- rep(0,l) # error metric 
        bootSig <- rep(0,l) # Number of model variables significant at bootstrap p-val
        paramSig <- rep(0,l) # Number of model variables significant at parametric p-val
        isEligeable <- rep(FALSE, l) # Is variable elligeable to be added to model
        numdatapts <- rep(0, l) # number of data on which model can be fitted
        
        for (j in 1:l) {
          
          ## consider the model with this variable added
          currModel <- c(inModel, candidVars[j])
          
          ## fit a linear model
          newModel <- lm(responseVar ~., train.data[,currModel])
          
          ## compute model metrics
          # Validation error
          pred <- predict(newModel, valid.data)
          #cat("Is the issue here?")
          error[j] <- mean((valid.data[,"responseVar"] - pred)^2, na.rm = TRUE)
          
          # parametric (based on normality assumption) significance
          paramSig[j] <- sum(summary(newModel)$coefficients[,4] < pval)
          
          # boostrap estimates of significance
          if (varSig == "bootstrap") {
            I <- rep(TRUE,nrow(train.data))
            for (k in 1:length(currModel)) { I <- I & !is.na(train.data[,currModel[k]])}
            #cat("bootstraping on ", sum(I), "data points\n")
            bootestim <- boot(data = train.data[I,currModel], statistic = boot.fn, 
                              R = 1000, responseVar = "responseVar")
            for (k in 1:length(currModel)) {
              bootCI <- boot.ci(bootestim, index = k, conf = (1-pval), type = "perc")$percent
              bootSig[j] <- bootSig[j] + (bootCI[4] <= 0 & bootCI[5] >= 0)
            }
          }
          
          # Number of data points and eligeability
          I <- rep(TRUE,N)
          for (k in 1:length(currModel)) { I <- I & !is.na(data[,currModel[k]]) }
          numdatapts[j] <- sum(I)
          
          # elligeability
          isEligeable[j] <- numdatapts[j] >= minCountries & error[j] < currPerformance
          if (varSig == "parametric") {
            isEligeable[j] <- isEligeable[j] & paramSig[j] == length(currModel)
          }
          if (varSig == "bootstrap") {
            isEligeable[j] <- isEligeable[j] & bootSig[j] == length(currModel)
          }
        }
        
        # determine which variable to add next to the model
        if (sum(isEligeable) == 0) { break } # stop if no variable is significant any more
        
        toAdd <- which.min(error[isEligeable])
        VartoAdd <- candidVars[isEligeable][toAdd]
        currPerformance <- min(error[isEligeable])
        
        inModel <- c(inModel, VartoAdd)
        candidVars <- candidVars[candidVars != VartoAdd]
        
        # save summary info for this model
        #cat("number of data points", numdatapts[isEligeable][toAdd])
        models$numData[i] <- numdatapts[isEligeable][toAdd]
        
        # update the cap
        I <- rep(TRUE, nrow(train.data)) 
        for (mindex in 1:length(inModel)) { I <- I & !is.na(train.data[,inModel[mindex]])}
        cap <- sum(I) - 1
        
      }
      
      # save summary info for this model before moving on to the next fold
      modelVars <- c(modelVars, list(inModel))
      models$CV_MSE[i] <- currPerformance
      
      if (length(inModel) > 1) {
        models$fullAdjRsquared[i] <- summary(lm(responseVar~., data[,inModel]))$adj.r.squared
        
        finModel <- lm(responseVar~. , train.data[,inModel]) # fit on training
        pred <- predict(finModel, valid.data) # predict on validation data
        models$CV_SMAPE[i] <- sum(2* abs(valid.data[,"responseVar"] - pred)/
                                    (abs(valid.data[,"responseVar"]) + abs(pred)), na.rm = TRUE)/sum(!is.na(pred))
        
        I <- !is.na(pred)
        TSS <- sum((valid.data[I,"responseVar"] - mean(train.data[,"responseVar"]))^2)
        RSS <- sum((valid.data[I,"responseVar"] - pred[I])^2)
        models$CV_Rsquared[i] <- 1 - (RSS/TSS)
      }
      
      cat("\n\n\n")
    }
    
    return(list(models, modelVars))
  }







# ============================================================================= #
# 
# This function plots two variables against each other and colours the points 
# based on the value of a third variable. Four colours are used, where each 
# colour corresponds to a quartile of the distribution of the third variable.
# Missing values will be default be black. 
# It also uses a different shape for the points based on each value of the
# third variable, namely triangle, square, circle, cross corresponding to
# 1st through 4th quartiles respectively.
#
# This function takes the following arguments:
# x <- a vector of data values to appear on the x-axis
# xlab <- the label for the x-axis on the plot
# xlim <- a vector with the minimum and maximum x-axis values
# y <- a vector of data values to appear on the y-axis
# ylab <- the label for the y-axis on the plot
# ylim <- a vector with the minimum and maximum y-axis values
# c <- the third variable to be used for colouring the plot
# sc <- a summary statistic of the distribution of c. This is the output of
#     the summary() function. The values of the quartiles are extracted from 
#     this vector.
# colour <- a vector of four colours (as strings) to be used for colouring,
#     in ourder from the bottom quartile to the top quartile.
# ============================================================================= #

make_coloured_xy_plot <- function(x, xlab, xlim, y, ylab, ylim, c, sc, colour) {
  
  # plots points missing values for c as empy circles
  I <- is.na(c)
  plot(c(0,x[I]),c(0,y[I]),xlab=xlab,ylab=ylab,xlim=xlim,ylim=ylim,cex=2.3,cex.lab=2.3,cex.axis = 2.3)
  abline(0,1)
  grid()
  
  I <- c < sc[2] 
  points(x[I],y[I],bg=colour[1],pch=24,cex=2.3,cex.lab=2.3,cex.axis = 2.3) # triangle
  
  I <- c >= sc[2] & c < sc[3] 
  points(x[I],y[I],bg=colour[2],pch=22,cex=2.3,cex.lab=2.3,cex.axis = 2.3) # square
  
  I <- c >= sc[3] & c < sc[5]
  points(x[I],y[I],bg=colour[3],pch=20,cex=2.3,cex.lab=2.3,cex.axis = 2.3) # circle
  
  I <- c >= sc[5]
  points(x[I],y[I],bg=colour[4],pch=4,cex=2.3,cex.lab=2.3,cex.axis = 2.3) # cross
}





# ============================================================================= #
# This function regresses a variable y on another variable x where the 
# variable x is de-biased using another variable c. It essentially runs
# the regression y = constant + coefficient*(de-biased x variable) where
# the (de-biased x variable) = (1 + k*(1-c))*x for some constant k and some 
# variable c which is assumed to be in [0,1].
# It runs this regression for different values of k (provided as input) and
# returns a data frame with two columns, one for the values of k and the other
# for the adjusted r-squared of the model resulting from using that value of k.
#
# It takes the following arguments:
# y <- the response variable in the regression
# x <- the explanatory variable
# c <- the variable to be used for de-biasing x
# k <- values of the bias correction factor to try
# ============================================================================= #
regress_with_correction_factors <- function(x, y, c, k) {
  
  # save the results
  results <- data.frame(k = k, adjR_squared = 0)
  
  # debias x for different values of k and record model performance
  for (i in 1:length(k)) {
    CF <- 1 + k[i]*(1-c)
    x_corrected <- x*CF
    I <- !is.na(x_corrected) & x_corrected > 1
    x_corrected[I] <- 1
    
    dat <- data.frame(y = y, x_corrected = x_corrected)
    model0 <- lm(y ~ x_corrected,dat)
    results$adjR_squared[i] <- summary(model0)$adj.r.squared
  }
  
  return(results)
}





# ============================================================================= #
# 
# This function peforms LOOCV. It takes as input the dataset containing the
# response variable and the explanatory variables in the model and fits the 
# model to all except one point of the data each time and reports the
# prediction on the data point left out. The following are the input arguments
# 
# Input:
# dataset <- the dataset to fit the model on
# response <- Name of the response variable in the dataset
# indicators <- Name of indicator variables (they are not scaled)
# And the output is:
# predictions <- a vector giving the predictions for each point in
#   the dataset from LOOCV (in the same order as the rows of the data)
#
# ============================================================================= #
predictLOOCV <- function(dataset, response, indicators) {
  
  # for convenience changes the name of the response variable
  colnames(dataset)[colnames(dataset) == response] <- "response_var"
  
  # go through the dataset, leaving out one observations at a time
  # as the test set
  predictions <- c() # vector to record the predictions
  n <- nrow(dataset)
  for (i in 1:n) {
    # leave out row i as test data
    train.data <- dataset[-i,]
    test.data <- dataset[i,]
    
    # scale all the variables
    # columns to be scaled (only the inputs that are not indicator variables)
    Idx <- !is.element(colnames(train.data), c("response_var",indicators)) 
    train.scaled <- scale(train.data[,Idx])
    train.data[,Idx] <- train.scaled[,]
    
    scale.center <- attr(train.scaled, "scaled:center") # centering value
    scale.scale <- attr(train.scaled, "scaled:scale")   # std. dev. value
    
    test.data[,Idx] <- scale(test.data[,Idx], 
                             center = scale.center, scale = scale.scale)[,]
    
    # fit the model to the training data
    model <- lm(response_var ~ . , train.data)
    
    # predict on the test data
    pred <- predict(model, test.data)
    predictions <- c(predictions, pred)
    #cat("how many predictions: ",length(pred)," -- ")
  }
  return(predictions)
}


# ============================================================================= #
# 
# This function takes as input:
# dataset <- the dataset containing the explanatory and response variables
# Indices <- a vector of the indices of the rows of the dataset to be used
# responseVar <- the name of the response variable
# 
# It fits a linear regression model of responseVar on all variables in the 
# dataset (using only the subset specified by Indices) and an intercept. 
# It returns the coefficients of the fitted linear model.
# This function is to be used during calls to bootstrap functions if it
# is desired to acquire bootstrap estimates of coefficient standard errors.
# ============================================================================= #
boot.fn <- function(dataset, Indices, responseVar) {
  
  # the response variable
  colnames(dataset)[colnames(dataset) == responseVar] = "Response"
  
  return(coef(lm(Response ~ . , data = dataset, subset = Indices)))
}

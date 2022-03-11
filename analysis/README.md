# Monthly Analysis
## Prerequisites
Ensure that `data/dgg-data-python` is on the python path so that modules from it can be imported.
The pycharm projects files have this preset.

A python 3.6+ virtual environment is set up and the `pycountry` package is downloaded.

R 3.5 is installed in windows. (Further work to be done to switch to a modern R wrapper with linux support)

## Input files
`DGG_Offline_dataset_compiled_Nov_2019.csv`, provided in folder `input`

`*_counts_20??_??_??.csv`, count files in folder `input/counts`, in the standard collection format, optional as the default task will download these from the bucket `sql_export` folder.

## Preprocessing
`generate_monthly_averages` reads all csv files in a month folder e.g. `2022-02` under `input/counts` and averages each variable collected across the month and stores it in memory in the json estimates format.

This is then passed to `preprocess_counts` which calculates ratios of age ranges and device use (see function for list of ratios).

A composite csv is saved in `input/counts` with the averaged values and calculated ratios e.g. `mau_monthly_counts_2022-02-01.csv`.

## Prediction
The R script in `source` is passed this composite csv file as input. Its model data is stored in `models`.

## Output files
Output will be saved to a folder under `output`, e.g. `2022-02`.
`Appendix_table_model_predictions.csv` is the main predictions file used by the website to display a given analysis.
`fits.csv` contains the model fit data for the given analysis.

## Running the monthly analysis
Running `monthly.py` will run an analysis for `2022-02` locally. Changing the class in `monthly_analysis_task` from `MonthlyAnalysis` to `MonthlyAnalysisBucket` will automatically upload results to the bucket and update the monthly analysis index.

# MLOps pipeline based on MicroService Architecture

Usage:
- Install apache kafka  
- Create the following topics: `data_preprocessing`, `feature_extraction`, `training`, `model_management` and `inference`  
- Create `config.yml` (See `specification.yaml` for all possible options)
- Data must be a single csv file

TODO:
- Create BASH setup script
- Read file extension for dataset and import accordingly
- Improve error logging and handling

Notes:
- Only English stopwords and letters are supported
- Data preprocessing steps must be in order
- If an exception occurs in a column during a data preprocessing step, it is not performed on subsequent columns
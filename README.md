# MLOps pipeline based on MicroService Architecture

## Docker Usage (Recommended)
- Ensure you have docker and docker compose.  
- run `setup.sh` to start the containers. Use `setup.sh --logs` or `setup.sh -l` to show logs in streaming mode after starting the containers.  
- When stopping the containers, use `docker compose stop` and NOT `docker compose down`. The data stored by kafka in the volume can only be read by the same container(same containerID).  
- If you have removed the containers, or want to remove the containers and all kafka data, run `clean.sh`.     

## Usage:
- Install apache kafka  
- Create the following topics: `data_preprocessing`, `feature_extraction`, `training`, `model_management` and `inference`  
- Create `config.yml` (See `specification.yaml` for all possible options)
- Data must be a single csv file

## TODO:
- Read file extension for dataset and import accordingly
- Improve error logging and handling
- Restore IO buffering in Dockerfile(s) after creating logging

Notes:
- Only English stopwords and letters are supported
- Data preprocessing steps must be in order
- If an exception occurs in a column during a data preprocessing step, it is not performed on subsequent columns
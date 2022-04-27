## Citibike Machine Learning Hands-on-Lab with Snowpark Python  

### Requirements:  
-A [Snowflake Trial Account](https://signup.snowflake.com/).  Note down the account identifier to be used in the hands-on-lab.  
-An [Amazon SageMaker Studio Lab Account](https://studiolab.sagemaker.aws/).  Do this ahead of time as sign-up may take up to 24 hours due to backlog.  
-Optional: Docker runtime environment such as [Docker Desktop](https://www.docker.com/products/docker-desktop/) or [Colima](https://github.com/abiosoft/colima) will be used for running and managing Apache Airflow DAGs.  Alternatively, if you do not have Docker, you will be able to create and run the ML Ops pipeline from Python.  
  
### Example Use-Case
In this example we use the [Citibike dataset](https://ride.citibikenyc.com/system-data). Citibike is a bicycle sharing system in New York City. Everyday users choose from 20,000 bicycles at over 1000 stations around New York City.

To ensure customer satisfaction Citibike needs to predict how many bicycles will be needed at each station. Maintenance teams from Citibike will check each station and repair or replace bicycles. Additionally, the team will relocate bicycles between stations based on predicted demand. The operations team needs an application to show how many bicycles will be needed at a given station on a given day.  
  
For this demo flow we will assume that the organization has the following **policies and processes** :   
-**Dev Tools**: Each user can develop in their tool of choice (ie. VS Code, IntelliJ, Pycharm, Eclipse, etc.).  Snowpark Python makes it possible to use any environment where they have a python kernel.  
-**Data Governance**: To preserve customer privacy no data can be stored locally.  The ingest system may store data temporarily but it must be assumed that, in production, the ingest system will not preserve intermediate data products between runs. Snowpark Python allows the user to push-down all operations to Snowflake and bring the code to the data.   
-**Automation**: Although the ML team can use any IDE or notebooks for development purposes the final product must be python code at the end of the work stream.  Well-documented, modularized code is necessary for good ML operations and to interface with the company's CI/CD and orchestration tools.  
-**Compliance**: Any ML models must be traceable back to the original data set used for training.  The business needs to be able to easily remove specific user data from training datasets and retrain models.  
  
  
### Setup Steps:

-Login to [SageMaker Studio Lab](https://studiolab.sagemaker.aws/).  
-Create a Runtime if there isn't one already  
-Click on Start Runtime  
-Click on Open Project  
-Select Git -> Clone Git Repository and enter the following:  
    -- Repository URL: https://github.com/sfc-gh-mgregory/snowpark-python-hol.  
-Select Yes when prompted to create a conda environment.  
-A terminal will open and create the environment.  When it is done run `conda activate snowpark_050` in the terminal window.  
-When opening notebooks be sure to select the "snowpark_050" kernel.  

### Alternative Client  

As an alternative to SageMaker Studio Lab this hands-on-lab can be run in Jupyter from local systems.  The following example is for MacOS.
```bash
curl https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh -o ~/Downloads/miniconda.sh  
sh ~/Downloads/miniconda.sh -b -p $HOME/miniconda  
~/miniconda/bin/conda init  
conda update conda
cat ~/.bash_profile >> ~/.zshrc  
. ~/.zshrc
```
-If git is not installed on your local system you can install via conda.
```bash
conda install git
```
-Clone this repository and create an environment
```
mkdir ~/Desktop/snowpark-python
cd ~/Desktop/snowpark-python
git clone https://github.com/sfc-gh-mgregory/snowpark-python-hol
cd snowpark-python-hol
conda env create -f jupyter_env.yml
conda activate snowpark_050
jupyter notebook
```

## TODO

-Update with public conda install of snowpark client  
-Alt client for Windows and linux  
-Table functions  
-Weather from marketplace  


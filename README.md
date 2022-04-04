## Citibike Machine Learning Hands-on-Lab with Snowpark Python  

### Requirements:  
-An account in [Amazon SageMaker Studio Lab](https://studiolab.sagemaker.aws/).  Do this ahead of time as sign-up may take up to 24 hours due to backlog.  
-A Snowflake account activated for Snowpark Private Preview.  
    

### Setup Steps:

-Login to SageMaker Studio Lab  
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
-

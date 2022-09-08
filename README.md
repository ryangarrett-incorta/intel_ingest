# intel_ingest
 Setup requirements for Incorta Intelligent Ingest

 # prequisites

 -Incorta 5.22 installed

 -JAVA_HOME set

 -Incorta Analytics & Loader Services created

 -core-site.xml file in the installer completed with your specific Azure details before running installer

# How to run
 vi ~/.baschrc

add to bashrc with your home directory example:  export INCORTA_HOME=/home/incorta/IncortaAnalytics/

source ~/.baschrc

Copy all files in this package to server with Incorta installed

chmod 755 install_requirements.py

./install_requirements.py /path/to/yourfile/core-site.xml

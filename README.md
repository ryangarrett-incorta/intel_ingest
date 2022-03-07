# intel_ingest
 Setup requirements for Incorta Intelligent Ingest

 #prequisites
 Incorta 5.14 Installed
 Java home set

 vi ~/.baschrc
export INCORTA_HOME=/home/incorta/IncortaAnalytics/

source ~/.baschrc

Copy all files in this package to server with Incorta installed

chmod 755 install_requirements.py

./install_requirements.py /path/to/yourfile/core-site.xml

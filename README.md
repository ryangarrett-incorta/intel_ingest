# intel_ingest
 Setup requirements for Incorta Intelligent Ingest

 #prequisites
 -Incorta 5.14 min Installed
 -JAVA_HOME set
 -Incorta Analytics & Loader Services created

 vi ~/.baschrc
export INCORTA_HOME=/home/incorta/IncortaAnalytics/

source ~/.baschrc

Copy all files in this package to server with Incorta installed

chmod 755 install_requirements.py

./install_requirements.py /path/to/yourfile/core-site.xml

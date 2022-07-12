FROM apache/airflow:2.2.4
USER root

RUN apt-get update && \
	apt install -y dirmngr gnupg apt-transport-https ca-certificates software-properties-common && \
	apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
add-apt-repository -y 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/' && \
	apt install -y r-base

RUN apt-get install -y libxml2-dev libcurl4-openssl-dev libssl-dev wget
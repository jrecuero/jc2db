#!/bin/bash

CURRENT_DIR=`pwd`
REPO_PATH=${HOME}/Repository
JC2LI_PATH=${REPO_PATH}/jc2li
JC2DB_PATH=${REPO_PATH}/jc2db/jc2db
VENV_PATH=${HOME}/virtualenv
P3_PATH=${VENV_PATH}/python3/bin/
PYTHON=python
RUN=${JC2DB_PATH}/run.py

export PYTHONPATH=$PYTHONPATH:.:${JC2LI_PATH}

${P3_PATH}/${PYTHON} ${RUN}

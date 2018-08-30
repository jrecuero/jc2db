#!/bin/bash

CURRENT_DIR=`pwd`
REPO_PATH=${HOME}/Repository
JC2CLI_PATH=${REPO_PATH}/jc2cli
JC2DB_PATH=${REPO_PATH}/jc2db/jc2db
CLI_PATH=${REPO_PATH}/jc2db/examples/cli
VENV_PATH=${HOME}/virtualenv
P3_PATH=${VENV_PATH}/python3.7/bin/
PYTHON=python
RUN=${CLI_PATH}/run.py

export PYTHONPATH=$PYTHONPATH:.:${JC2CLI_PATH}

${P3_PATH}/${PYTHON} ${RUN}

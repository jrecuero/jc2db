#!/bin/bash

CURRENT_DIR=`pwd`
REPO_PATH=${HOME}/Repository
JC2DB_PATH=${REPO_PATH}/jc2db
JC2LI_PATH=${REPO_PATH}/jc2li

export PYTHONPATH=$PYTHONPATH:$JC2LI_PATH

vim $1

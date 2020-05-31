#!/bin/bash

. ~/data_etls/venv/bin/activate
export PYTHONPATH=~/data_etls
~/data_etls/venv/bin/python3 ~/data_etls/$1/$2 $3


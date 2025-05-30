#!/bin/bash
set -ex

sudo yum install -y python3 python3-pip

sudo pip3 install --no-cache-dir pandas seaborn matplotlib boto3 numpy

echo "Paquetes instalados correctamente"
#!/bin/bash

# !!!! Install google-cloud-cli if not installed !!!!
# sudo apt update
# sudo apt install apt-transport-https ca-certificates gnupg curl
# curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
# echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
# sudo apt update && sudo apt install google-cloud-cli

python3.11 -m venv venv_composer
source venv_composer/bin/activate

pip install git+https://github.com/GoogleCloudPlatform/composer-local-dev.git
pip install requests==2.31.0

composer-dev create \
  --from-image-version composer-2.9.11-airflow-2.10.2 \
  --project ... \
  --dags-path ./dags \
  google_composer_local

cp features/composer_dev/requirements.txt composer/google_composer_local/requirements.txt
cp features/composer_dev/variables.env composer/google_composer_local/variables.env

composer-dev start google_composer_local

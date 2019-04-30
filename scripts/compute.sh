#!/bin/bash

#sudo gcloud auth login
sudo gcloud config set project instant-amp-237719
sudo gcloud config set zone us-central-a
sudo gcloud compute ssh turing-m

# ssh turing-m.us-central1-a.instant-amp-237719

gcloud compute --project "instant-amp-237719" ssh --zone "europe-west2-c" "turing-m"

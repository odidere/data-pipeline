#!/bin/sh

args=$@
for element in $args
do
  echo "cloning $element"
  git config --global github.user odidere
  git config --global github.username odidere
  timeout 60s git clone $element.git || true
done
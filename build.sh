#!/bin/bash

set -eu

sbt assembly

docker build -t tutum.co/partup/api .

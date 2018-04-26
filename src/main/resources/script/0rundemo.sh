#!/usr/bin/env bash
sh ~/workspace/tssdemov2/src/main/resources/script/1emptydir.sh
sh ~/workspace/tssdemov2/src/main/resources/script/2createdir.sh
sh ~/workspace/tssdemov2/src/main/resources/script/3ingestion.sh
#sh ~/workspace/tssdemov2/src/main/resources/script/startservices.sh
sh ~/workspace/tssdemov2/src/main/resources/script/4transform.sh
#sh ~/workspace/tssdemov2/src/main/resources/script/5impalaquery.sh
#sh ~/workspace/tssdemov2/src/main/resources/script/6hbasewithspark.sh

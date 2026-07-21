#!/bin/bash

#Because the Google Cloud bulk import from URLs to bucket
#COPIED THE ENTIRE DEEPLY NESTED DIRECTORY STRUCTURE from
#dumps.wikimedia.org

for d in 2026-01 2026-02 2026-03 2026-04; do
	echo $d
	gcloud storage mv gs://phonic-envoy-499516-i9-cs131/clickstreams/dumps.wikimedia.org/other/clickstream/$d/clickstream-enwiki-$d.tsv.gz gs://phonic-envoy-499516-i9-cs131/clickstreams/
done

for year in 2018 2019 2020 2021 2022 2023 2024 2025; do
        echo $year
	for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
		echo $month
        	gcloud storage mv gs://phonic-envoy-499516-i9-cs131/clickstreams/dumps.wikimedia.org/other/clickstream/$year-$month/clickstream-enwiki-$year-$month.tsv.gz gs://phonic-envoy-499516-i9-cs131/clickstreams/
	done
done

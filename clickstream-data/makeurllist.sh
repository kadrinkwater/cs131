#!/bin/bash


out="urllist.tsv"

# The first line must specify TsvHttpData-1.0

echo "TsvHttpData-1.0" >> $out

# Example: https://dumps.wikimedia.org/other/clickstream/2026-05/clickstream-enwiki-2026-05.tsv.gz

function appendurl {
	echo "https://dumps.wikimedia.org/other/clickstream/$1-$2/clickstream-enwiki-$1-$2.tsv.gz" >> $out
}



# Year range: 2018-2025
# Month range: 01-12
# Also includes 2017-11, 2017-12, and 2026- 01 thru 06

appendurl 2017 11
appendurl 2017 12

for year in 2018 2019 2020 2021 2022 2023 2024 2025; do
	for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
		appendurl $year $month
	done
done

for month in 01 02 03 04; do
	appendurl 2026 $month
done

#sort $out -o $out

echo "finished!"
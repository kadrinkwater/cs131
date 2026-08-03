#!/bin/bash


out="datelist.txt"

# Example: https://dumps.wikimedia.org/other/clickstream/2026-05/clickstream-enwiki-2026-05.tsv.gz

function add {
	echo "$1-$2" >> $out
}



# Year range: 2018-2025
# Month range: 01-12
# Also includes 2017-11, 2017-12, and 2026- 01 thru 06

add 2017 11
add 2017 12

for year in 2018 2019 2020 2021 2022 2023 2024 2025; do
	for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
		add $year $month
	done
done

for month in 01 02 03 04 05 06; do
	add 2026 $month
done

#sort $out -o $out

echo "finished!"

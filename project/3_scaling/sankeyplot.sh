#!/bin/bash

# Script to generate a Sankey flow diagram for a given Wikipedia page title.


# Use awk to find lines containing the page title as curr,
# and output a list of prev - [page title] - number

# Use awk to find lines containing the page title as prev,
# and output a list of [page title] - curr - number

# Call python script to use plotly to make actual image
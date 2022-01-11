#!/bin/bash
echo "do JobA"
dayOfTheWeek=`date +%w`
echo "{\"dayOfTheWeek\":$dayOfTheWeek}" > $JOB_OUTPUT_PROP_FILE

#!/bin/bash
dotfiles=`ls | grep .dot | cut -d '.' -f 1`

for eachfile in $dotfiles
do
   part1='dot -Tsvg -o '
   part2='.svg '
   part3='.dot'
   completecommand="${part1}${eachfile}${part2}${eachfile}${part3}"

   eval $completecommand
done
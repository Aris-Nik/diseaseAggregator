#/bin/bash

mapfile -t diseases < $1.txt
mapfile -t countries < $2.txt
firstName=("John" "George" "Michael" "Thomas" "Spencer" "Zane" "Miles" "Cohen" "Cayden" "Sean" "Marcel" "Ellis" "Jack" "Jasper" "Otto" "Alan" "Abel" "David" "Rowan" "Jay" "Ryan" "Christopher")
lastName=("Kennedy" "Bush" "Jackson" "Riley" "Johnson" "May" "Scott" "Chapman" "Cook" "Mcdonald" "Gardner" "Young" "Black" "Hayes" "Chambers" "Carter" "Wells" "Barnes" "Lane")
mkdir "$3"
for each in "${diseases[@]}"
do
  echo "$each"
done
printf "\n"
id=0
cd $3
temp="-"
temp2=" "
enter="Enter"
for each in "${countries[@]}"
do
  mkdir $each
  cd $each
  for ((i=0;i<$4;i++))
  do
	day=$((1 + RANDOM % 30))
	if [ $day -lt 10 ]
	then
		day="0$day"
	fi
	month=$((1 + RANDOM % 12))
	if [ $month -lt 10 ]
	then
		month="0$month"
	fi
	year=$((2000 + RANDOM % 20))
	out="$day$temp$month$temp$year.txt"
	#echo >> "$out"
	tempj=0
	for ((j=0;j<$5;j++))
	do
		age=$((1 + RANDOM % 120))
		rand_index=$[$RANDOM % ${#diseases[@]}]
		rand_disease=${diseases[$rand_index]}
		rand_index=$[$RANDOM % ${#firstName[@]}]
		rand_fname=${firstName[$rand_index]}
		rand_index=$[$RANDOM % ${#lastName[@]}]
		rand_lname=${lastName[$rand_index]}
		rand_rand=$((RANDOM % 2))
		if [ $rand_rand -lt 1 ]
		then
			tempj=$[tempj+2]
			echo "$id ENTER $rand_fname $rand_lname $rand_disease $age" >> "$out"
			echo "$id EXIT $rand_fname $rand_lname $rand_disease $age" >> "$out"
		else
			tempj=$[tempj+1]
			echo "$id EXIT $rand_fname $rand_lname $rand_disease $age" >> "$out"		
		fi
		if [ $tempj -eq $5 ]				#if records are equal to the records that user gave then dont add more
		then
			break
		fi
		if [ $tempj -gt $5 ]				#if we exceeded the limit then we exceeded it by 1 so remove the last line and we are ok :)
		then
			sed -i '$ d' "$out"
			break
		fi
		id=$[id + 1]
	done
  done
  cd ..
done

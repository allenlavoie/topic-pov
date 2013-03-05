TOPICS=$2
POV=$3
bin/initialize $1 $TOPICS $POV $4 0.8 0.2 0.2 0.8 0.1 $(echo "5/($TOPICS * $POV)" | bc -l)
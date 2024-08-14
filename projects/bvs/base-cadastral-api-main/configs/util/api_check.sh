api=$1
num=$2
filter=$3

if [ "$num" = "1" ]
then
    nums="[0-9] "
elif [ "$num" = "2" ]
then
    nums="[0-9][0-9] "
elif [ "$num" = "3" ]
then
    nums="[0-9][0-9][0-9] "
elif [ "$num" = "4" ]
then
    nums="[0-9][0-9][0-9][0-9] "
elif [ "$num" = "5" ]
then
    nums="[0-9][0-9][0-9][0-9][0-9] "
elif [ "$num" = "6" ]
then
    nums="[0-9][0-9][0-9][0-9][0-9][0-9] "
else
    nums="*"
fi

path="${CADASTRAL_API_LOCAL_LOG}/$api-api-cadastral-requests*.log"

if [ -z "$filter" ]; then
        cat $path | grep "elapsedtime=$nums"
else
        cat $path | grep "$filter".*:: | grep "elapsedtime=$nums"
fi


if [ -z "$1" ]; then
        filter=""
else
        filter="$1"
fi

if [ -z "$2" ]; then
        api=rest
else
        api="$2"
fi

clear;echo "Checking $filter..."
path=${CADASTRAL_API_LOCAL_UTIL}/api_check.sh

timeout=`sh $path $api 4 $filter | grep "statuscode=408" | wc -l`
errors=`sh $path $api 4 $filter | grep "statuscode=500" | wc -l`
ms1=`sh $path $api 1 $filter | wc -l`
ms2=`sh $path $api 2 $filter | wc -l`
ms3=`sh $path $api 3 $filter | wc -l`
ms4=`sh $path $api 4 $filter | wc -l`
ms5=`sh $path $api 5 $filter | wc -l`
ms6=`sh $path $api * $filter | wc -l`
total=`echo "$ms1 + $ms2 + $ms3 + $ms4 + $ms5 + $ms6" | bc`

if [ $errors == "0" ] && [ $timeout == "0" ] ; then
        success=$total
else
	total_errors=`echo "$errors + $timeout" | bc`
        success=`echo "$total - $total_errors" | bc`
fi

echo ""
echo "date run: `date '+%Y-%m-%d %H:%M:%S'`"

echo "`printf %05s API`|`printf %10s Date`|`printf %08s Total`|`printf %08s Success`|`printf %08s Errors`|`printf %08s Timeout`|`printf %08s 0-9ms`|`printf %08s 10-99ms`|`printf %09s 100-999ms`|`printf %5s 1s-9s`|`printf %07s 10s-90s`|`printf %03s \>90s`"

echo "---------------------------------------------------------------------------------------------------"

echo "`printf %05s $api`|`printf %08s $filter`|`printf %08s $total`|`printf %08s $success`|`printf %08s $errors`|`printf %08s $timeout`|`printf %08s $ms1`|`printf %08s $ms2`|`printf %09s $ms3`|`printf %5s $ms4`|`printf %07s $ms5`|`printf %03s $ms6`"

echo ""

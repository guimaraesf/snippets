ms=4
s=9
path=${CADASTRAL_API_LOCAL_UTIL}/api_check.sh

if [ -z "$1" ]; then
        filter=`date +%Y-%m-%d`
else
        filter=$1
fi

clear;echo "Checking rest $filter..."

for ((i=ms; i>=1; i--))
do
        result=`sh $path rest $i $filter | wc -l`

        if [ $result -eq 0 ]; then
                continue;
        fi

        for ((j=s; j>=1; j--))
        do
            result=`sh $path rest $i $filter | grep elapsedtime=$j | head -n 1`

            if [ ! -z "$result" ]; then
                x=`expr $i - 1`
                value=`printf "%0${x}d${j}" | rev`
                echo "Response time max: ~${value}ms"
                echo "$result"
                exit 0;
            fi
        done

done
echo "Done."


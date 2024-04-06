day=`date +%Y-%m-%d`

if [ -z "$1" ]; then
        filter=$day
else
        filter="$1"
fi

sh /home/svc_cadastral_api/api/util/relatorio.sh $filter | tail -n 1 >> /tmp/relatorio_cadastral/relatorio.csv

#!/bin/bash
set -e

################################################
#########      INICIO DA EXECUCAO      #########
################################################
if [ "$1" == "" ] || [ "$2" == "" ] || [ "$3" == "" ] || [ "$4" == "" ] || [ "$5" == "" ] ; then
    echo "Numero de parametros invalido!"
    echo "sintaxe: $(basename "$0") <versão> <rest_port> <interval_check_up> <max_check_up> <tail_lines_error_log>"
    exit 1
fi

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

version=$1
rest_port=$2
interval_check_up=$3
max_check_up=$4
tail_lines_error_log=$5

function status(){
    local message=$1
    local level=$( (( $# == 2 )) && echo "$2" || echo "INFO")
    echo "[${level}] `date '+%Y-%m-%d-%H:%M:%S'` ${message}"
}

function make_backup_files() {
    local files=$1
    local ext_bkp=$2
    if ls $(echo "${files}") &>/dev/null; then
       for j in $(echo "${files}"); do mv -- "$j" "${j}_${ext_bkp}"; done
    fi
}

function make_backup() {
    local ext_bkp="bkp_`date '+%Y-%m-%d-%H-%M-%S'`"
    make_backup_files "${CADASTRAL_API_LOCAL_HOME}/*.jar" "${ext_bkp}"
    make_backup_files "${CADASTRAL_API_LOCAL_HOME}/*.yml" "${ext_bkp}"
    make_backup_files "${CADASTRAL_API_LOCAL_HOME}/*.xml" "${ext_bkp}"
    make_backup_files "${CADASTRAL_API_LOCAL_HOME}/*.sh" "${ext_bkp}"
    make_backup_files "${CADASTRAL_API_LOCAL_UTIL}/*.sh" "${ext_bkp}"
}

function do_deploy() {
    status "EXTRAINDO SCRIPS: ${CADASTRAL_API_LOCAL_HOME}"
    unzip -o ${SCRIPTPATH}/deploy_scripts.zip -d ${CADASTRAL_API_LOCAL_HOME}/

    status "EXTRAINDO SCRIPS: ${CADASTRAL_API_LOCAL_UTIL}"
    unzip -o ${SCRIPTPATH}/deploy_utils.zip -d ${CADASTRAL_API_LOCAL_UTIL}/

    status "ATUALIZANDO JAR"
    mv ${SCRIPTPATH}/core-api.jar ${CADASTRAL_API_LOCAL_HOME}/core-api-${version}.jar

    cd ${CADASTRAL_API_LOCAL_HOME}
    sh shutdown.sh
    sleep 3s; sh run.sh
    cd -
}

function check_api_is_up() {
    health_url="http://$(hostname):${rest_port}/api/health"
    status "CHECANDO STATUS API: ${health_url}"
    check_up=$(curl --header "Accept:application/json" ${health_url} | grep "UP" | wc -l)
    test ${check_up} -gt 0
}

status "EXECUTANDO DEPLOY COM VERSÃO: ${version}"

status "CONFIGURANDO VARIÁVEIS DE AMBIENTE"
cat ${SCRIPTPATH}/profile_cadastral > /etc/profile_cadastral_api
source ~/.bashrc; source /etc/profile;

mkdir -p ${CADASTRAL_API_LOCAL_HOME}

make_backup

do_deploy

count=1
while [ ${count} -lt ${max_check_up} ];
do
    if check_api_is_up; then
        status "DEPLOY EXECUTADO COM SUCESSO"
        exit 0
    else
        status "AGUARDAR ${interval_check_up} PARA NOVA TENTATIVA: ${count} de ${max_check_up}"
        sleep ${interval_check_up};
        count=$((count + 1))
    fi
done

status "NUMERO DE TENTATIVAS ATINGIDO: ${max_check_up}"
status "APLICAÇÃO NÃO INICIOU CORRETAMENTE, verifique os logs !" "ERROR"

tail -n ${tail_lines_error_log} ${CADASTRAL_API_LOCAL_LOG}/api-cadastral-root.log

exit 2
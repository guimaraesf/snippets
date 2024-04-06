export JMETER_HOME=/home/tr_ehuerta/jmeter/apache-jmeter-5.0
export JMETER_DOCS=/home/tr_ehuerta/jmeter

${JMETER_HOME}/bin/jmeter.sh -n -t ${JMETER_DOCS}/cadastral_pf_servicos.jmx -l ${JMETER_DOCS}/logs_api/api.log -o ${JMETER_DOCS}/output_test_load/

# chmod +x ${JMETER_HOME}/bin/*
# chmod +x ${JMETER_DOCS}/*.sh
# nohup ${JMETER_DOCS}/run_jmeter.sh &
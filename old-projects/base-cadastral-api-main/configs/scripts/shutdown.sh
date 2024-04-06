if [ -f "${CADASTRAL_API_LOCAL_HOME}/app.pid" ]; then
   kill $(cat ${CADASTRAL_API_LOCAL_HOME}/app.pid)
   rm ${CADASTRAL_API_LOCAL_HOME}/app.pid
fi

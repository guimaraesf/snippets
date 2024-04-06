package br.com.bs.ppe.infrastructure.repository.bigtable.connection.impl;

import br.com.bs.bigtable.config.BigTableConfig;
import br.com.bs.ppe.infrastructure.repository.bigtable.connection.BigTableConnection;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Profile("bigtable")
public class BigTableConnectionImpl implements BigTableConnection {

    private final Logger LOGGER = LoggerFactory.getLogger(BigTableConnectionImpl.class);

    private final BigTableConfig bigTableConfig;

    public BigTableConnectionImpl(BigTableConfig bigTableConfig) {
        this.bigTableConfig = bigTableConfig;
    }

    @Override
    public BigtableDataClient createDataClient() throws IOException {
        try {
            BigtableDataSettings settings = BigtableDataSettings.newBuilder()
                    .setProjectId(this.bigTableConfig.getProjectId())
                    .setInstanceId(this.bigTableConfig.getInstanceId())
                    .setAppProfileId(this.bigTableConfig.getAppProfile())
                    .build();
            return BigtableDataClient.create(settings);
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable", e);
            throw e;
        }
    }
}

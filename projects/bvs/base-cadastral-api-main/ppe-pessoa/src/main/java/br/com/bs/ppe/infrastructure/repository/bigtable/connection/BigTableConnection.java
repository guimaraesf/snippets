package br.com.bs.ppe.infrastructure.repository.bigtable.connection;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;

import java.io.IOException;

public interface BigTableConnection {

    BigtableDataClient createDataClient() throws IOException;
}

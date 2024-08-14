package br.com.bs.rest.config;

import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;

import java.io.IOException;


public class StackDriverTracer {
    public static void createAndRegisterGoogleCloudPlatform() throws IOException {
        StackdriverTraceExporter.createAndRegister(
                StackdriverTraceConfiguration.builder().setProjectId("dev-data-31ce").build());
    }
}

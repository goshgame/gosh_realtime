package com.gosh.util;

import com.gosh.entity.ApiResponse;
import com.gosh.entity.FeastRequest;
import com.gosh.process.FeastApi;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Flink Sink Function for writing FeastRequest to Feast online store.
 */
public class FeastSinkFunction extends RichSinkFunction<FeastRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(FeastSinkFunction.class);

    private String jobName;

    public FeastSinkFunction() {
        // Default constructor for Flink serialization
    }

    public FeastSinkFunction(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("FeastSinkFunction for job {} opened.", jobName != null ? jobName : "unknown");
    }

    @Override
    public void invoke(FeastRequest value, Context context) throws Exception {
        if (value == null) {
            LOG.warn("FeastRequest is null, skipping write to online store.");
            return;
        }
        ApiResponse<Map<String, Object>> response = FeastApi.writeToOnlineStore(value);
        if (response != null && response.isSuccess()) {
            LOG.debug("Successfully wrote to Feast online store for project: {}, featureView: {}", value.getProject(),
                    value.getFeatureViewName());
        } else {
            LOG.error("Failed to write to Feast online store for project: {}, featureView: {}. Response: {}",
                    value.getProject(), value.getFeatureViewName(), response != null ? response.toString() : "null");
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        LOG.info("FeastSinkFunction for job {} closed.", jobName != null ? jobName : "unknown");
    }
}
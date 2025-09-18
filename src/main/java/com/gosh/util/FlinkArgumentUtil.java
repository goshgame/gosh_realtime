package com.gosh.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkArgumentUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkArgumentUtil.class);

    /**
     * 初始化参数
     * @param args
     * @return
     * @throws Exception
     */
    private static CommandLine initArgument(String[] args) throws Exception {
        Options options = new Options();

        options.addOption("cf", "conf-file", true, "Path to flink custom yaml");
        options.addOption("sf", "sql-file", true, "Path to flink custom sql");

        CommandLineParser parser = new DefaultParser();

        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }

}

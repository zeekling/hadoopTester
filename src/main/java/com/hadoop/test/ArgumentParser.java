package com.hadoop.test;

import lombok.Getter;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;

public class ArgumentParser {

    private Options optList;
    private String[] argumentList;
    private ParsedOutput parsed;


    public ArgumentParser(String[] args) {
        optList = getOptions();
        if (args == null) {
            args = new String[]{};
        }
        argumentList = args;
        parsed = null;
    }

    private Options getOptions() {
        Options cliopt = new Options();
        cliopt.addOption(ConfigOption.MAPS);
        cliopt.addOption(ConfigOption.REDUCES);
        cliopt.addOption(ConfigOption.BASE_DIR);
        cliopt.addOption(ConfigOption.OPERATIONS);
        cliopt.addOption(ConfigOption.FILE_SIZE);
        cliopt.addOption(ConfigOption.FILE_COUNT);
        cliopt.addOption(ConfigOption.OPS_PER_MAPPER);
        cliopt.addOption(ConfigOption.DIR_COUNT);
        cliopt.addOption(ConfigOption.HELP);
        return cliopt;
    }

    ParsedOutput parse() throws Exception {
        PosixParser parser = new PosixParser();
        CommandLine popts = parser.parse(getOptionList(), argumentList, true);
        if (popts.hasOption(ConfigOption.HELP.getOpt())) {
            parsed = new ParsedOutput(null, this, true);
        } else {
            parsed = new ParsedOutput(popts, this, false);
        }
        return parsed;
    }

    private Options getOptionList() {
        return optList;
    }

    static class ParsedOutput {
        @Getter
        private CommandLine parsedData;
        private ArgumentParser source;
        private boolean needHelp;

        ParsedOutput(CommandLine parsedData, ArgumentParser source,
                     boolean needHelp) {
            this.parsedData = parsedData;
            this.source = source;
            this.needHelp = needHelp;
        }

        /**
         * @return whether the calling object should call output help and exit
         */
        boolean shouldOutputHelp() {
            return needHelp;
        }

        public Path getOutputPath() {
            String path = getValue(ConfigOption.BASE_DIR.getOpt());
            return new Path(path);
        }

        /**
         * Outputs the formatted help to standard out
         */
        void outputHelp() {
            if (!shouldOutputHelp()) {
                return;
            }
            if (source != null) {
                HelpFormatter hlp = new HelpFormatter();
                hlp.printHelp(Constants.PROG_NAME + " " + Constants.PROG_VERSION,
                        source.getOptionList());
            }
        }

        /**
         * @param optName the option name to get the value for
         * @return the option value or null if it does not exist
         */
        String getValue(String optName) {
            if (parsedData == null) {
                return null;
            }
            return parsedData.getOptionValue(optName);
        }

        String getValue(String optName, String defaultValue) {
            if (parsedData == null) {
                return defaultValue;
            }
            String optionValue = parsedData.getOptionValue(optName);
            if (optionValue == null) {
                return defaultValue;
            }
            return optionValue;
        }


        int getValueAsInt(String optName, int defaultValue) {
            String value = getValue(optName);
            if (value == null) {
                return defaultValue;
            }
            return Integer.parseInt(value);
        }

        public String toString() {
            StringBuilder s = new StringBuilder();
            if (parsedData != null) {
                Option[] ops = parsedData.getOptions();
                for (Option op : ops) {
                    s.append(op.getOpt()).append(" = ").append(s.append(op.getValue())).append(",");
                }
            }
            return s.toString();
        }

    }


}

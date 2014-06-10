package eu.scape_project.pt.mapred;

import eu.scape_project.pt.util.PropertyNames;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

/**
 * A command-line interaction wrapper to execute cmd-line tools with MapReduce.
 * Code based on SimpleWrapper.
 * 
 * @author Rainer Schmidt [rschmidt13]
 * @author Matthias Rella [myrho]
 */ 
public class CLIWrapper extends Configured implements org.apache.hadoop.util.Tool {

    private static Log LOG = LogFactory.getLog(CLIWrapper.class);

    @SuppressWarnings("static-access") // OptionBuilder uses the pattern of calling the static methods on instances, leave as is
    private Options buildOptions() {
        Options options = new Options();
        Option oH = OptionBuilder
            .withDescription("print help")
            .create("help");
        Option oI = OptionBuilder.withArgName("control file")
            .hasArg()
            .isRequired(true)
            .withDescription("specify the control file containing the toolspec, operation and parameter specifications")
            .create("i");
        Option oO = OptionBuilder.withArgName("output directory")
            .hasArg()
            .isRequired(false)
            .withDescription("specify the directory where results of the MapReduce job will be written to")
            .create("o");
        Option oN = OptionBuilder.withArgName("number of lines")
            .hasArg()
            .isRequired(false)
            .withDescription("specify the number of lines one mapper should get")
            .create("n");
        Option oR = OptionBuilder.withArgName("toolspec repository")
            .hasArg()
            .isRequired(true)
            .withDescription("specify the path to the toolspec repository")
            .create("r");

        options.addOption(oH);
        options.addOption(oI);
        options.addOption(oO);
        options.addOption(oN);
        options.addOption(oR);
        return options;
    }

    private String[] parseOptions(Configuration conf, String[] args) {
        CommandLineParser parser = new GnuParser();

        Options opts = buildOptions();
        try{
            CommandLine commandLine = parser.parse(opts, args, true);
            processOptions(conf, commandLine);
            return commandLine.getArgs();
        } catch(ParseException e){
            LOG.warn("options parsing failed: "+e.getMessage());

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("options are: ", opts);
        }
        return args;
    }

    private void processOptions(Configuration conf, CommandLine line) {
        if( line.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("options are: ", buildOptions());
            return;
        }
        if( line.hasOption("i")){
            //FileInputFormat.setInputPaths(conf, new Path(line.getOptionValue("i")));
            conf.set(PropertyNames.INFILE, line.getOptionValue("i"));
        }
        if( line.hasOption("o")){
            //FileOutputFormat.setOutputPath(new Path(line.getOptionValue("o")));
            conf.set(PropertyNames.OUTDIR, line.getOptionValue("o"));
        }
        if( line.hasOption("n")){
            //NLineInputFormat.setNumLinesPerSplit(
                //conf, 
                //Integer.parseInt(line.getOptionValue("n")));
            conf.set(PropertyNames.LINES_PER_MAP, line.getOptionValue("n"));
        }
        if( line.hasOption("r")){
            conf.set(PropertyNames.REPO_LOCATION, line.getOptionValue("r"));
        }

    }

    private void defaultOptions(Configuration conf) {
        if( conf.get(PropertyNames.REDUCE_CLASS ) == null ) {
            if( conf.get(PropertyNames.OUTPUT_KEY_CLASS) == null ) {
                conf.set(PropertyNames.OUTPUT_KEY_CLASS, "org.apache.hadoop.io.LongWritable");
            }
            if( conf.get(PropertyNames.OUTPUT_VALUE_CLASS) == null ) {
                conf.set(PropertyNames.OUTPUT_VALUE_CLASS, "org.apache.hadoop.io.Text");
            }
        }
        if( conf.get(PropertyNames.OUTDIR ) == null ) {
            conf.set(PropertyNames.OUTDIR, "out/"+System.nanoTime()%10000 );
        }
        if( conf.get(PropertyNames.INPUTFORMAT_CLASS ) == null ) {
            conf.set(PropertyNames.INPUTFORMAT_CLASS, "eu.scape_project.pt.mapred.input.ControlFileInputFormat");
        }

    }
    
    /**
     * Sets up, initializes and starts the Job.
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        parseOptions(conf, args);
        defaultOptions(conf);

        //hadoop's output 
        LOG.info("Output: " + conf.get(PropertyNames.OUTDIR));
        //toolspec directory
        LOG.info("Toolspec Directory: " 
                + conf.get(PropertyNames.REPO_LOCATION));
        //NInputFormat
        LOG.info("Number of Lines: " 
                + conf.get(PropertyNames.LINES_PER_MAP));

        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setMapperClass(ToolspecMapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }
    
    /**
     * CLIWrapper user interface. See printUsage for further information.
     */
    public static void main(String[] args) {
        
        int res = 1;
        CLIWrapper mr = new CLIWrapper();
                
        try {
            LOG.info("starting ...");
            res = ToolRunner.run(mr, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(res);
    }       

}

package eu.scape_project.pt.mapred;

import eu.scape_project.pt.repo.Repository;
import eu.scape_project.pt.repo.ToolRepository;
import eu.scape_project.pt.util.PropertyNames;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import eu.scape_project.pt.mapred.input.ControlFileInputFormat;

/**
 * A command-line interaction wrapper to execute cmd-line tools with MapReduce.
 * Code based on SimpleWrapper.
 * 
 * @author Rainer Schmidt [rschmidt13]
 * @author Matthias Rella [myrho]
 * @author Martin Schenck [schenck]
 */ 
public class CLIWrapper extends Configured implements org.apache.hadoop.util.Tool {

    private static Log LOG = LogFactory.getLog(CLIWrapper.class);
    
    /**
     * Sets up, initializes and starts the Job.
     */
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf);

        job.setJarByClass(CLIWrapper.class);

        job.setOutputKeyClass(LongWritable.class);
        // TODO Output Value Class may depend on the tool invoked
        job.setOutputValueClass(Text.class);

        if(conf.get(PropertyNames.TAVERNA_WORKFLOW) != null 
           && conf.get(PropertyNames.TAVERNA_WORKFLOW) != "") {
            job.setMapperClass(TavernaMapper.class);
        } else {
            job.setMapperClass(ToolspecMapper.class);
        }
        
        job.setInputFormatClass(ControlFileInputFormat.class);
        ControlFileInputFormat.setNumLinesPerSplit(
            job, 
            Integer.parseInt(conf.get(PropertyNames.NUM_LINES_PER_SPLIT)));
        
        // copy input file to temporary directory
        FileSystem fs = FileSystem.get(conf);
        Path fSrc = new Path(conf.get(PropertyNames.INFILE));
        Path fDst = new Path("/tmp/input-" + job.getJobName());
        fs.copyFromLocalFile(false, true, fSrc, fDst);

        FileInputFormat.addInputPath(job, fDst);
        FileOutputFormat.setOutputPath(job, new Path(conf.get(PropertyNames.OUTDIR)) ); 
                
        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }
    
    /**
     * CLIWrapper user interface. See printUsage for further information.
     */
    public static void main(String[] allArgs) throws Exception {
        
        int res = 1;
        CLIWrapper mr = new CLIWrapper();
        Configuration conf = new Configuration();
        Repository repo;
        String[] args = new GenericOptionsParser(conf, allArgs).getRemainingArgs();

        Map<String, String> parameters = new HashMap<String, String>() {{
            put("i", PropertyNames.INFILE );
            put("o", PropertyNames.OUTDIR );
            put("t", "mapred.job.reuse.jvm.num.tasks" );
            put("n", PropertyNames.NUM_LINES_PER_SPLIT );
            put("r", PropertyNames.REPO_LOCATION);
            put("v", PropertyNames.TAVERNA_HOME );
            put("w", PropertyNames.TAVERNA_WORKFLOW );
            put("j", "mapred.job.name" );
        }};
                
        try {
            String pStrings = "";
            for( String i : parameters.keySet() )
                pStrings += i + ":";

            OptionParser parser = new OptionParser(pStrings);
            OptionSet options = parser.parse(args);

            // default values:
            conf.set(PropertyNames.NUM_LINES_PER_SPLIT, "1");
            conf.set(PropertyNames.OUTDIR, "out/"+System.nanoTime()%10000 );

            // store parameter values:
            for( Entry<String, String> param : parameters.entrySet() )
                if(options.hasArgument(param.getKey()))
                    conf.set(param.getValue(), 
                             options.valueOf(param.getKey()).toString());

            LOG.info("Job name: " + conf.get("mapred.job.name"));
            //hadoop's output 
            LOG.info("Output: " + conf.get(PropertyNames.OUTDIR));
            //action to select
            LOG.info("Action: " + conf.get(PropertyNames.ACTIONSTRING));
            //toolspec directory
            LOG.info("Toolspec Directory: " 
                    + conf.get(PropertyNames.REPO_LOCATION));
            //jvm reuse
            LOG.info("JVM reuse: " 
                    + conf.get("mapred.job.reuse.jvm.num.tasks"));
            //NInputFormat
            LOG.info("Number of Lines: " 
                    + conf.get(PropertyNames.NUM_LINES_PER_SPLIT));
            // taverna workflow location
            LOG.info("Taverna: " + conf.get(PropertyNames.TAVERNA_HOME));
            // taverna home
            LOG.info("Workflow: " + conf.get(PropertyNames.TAVERNA_WORKFLOW));

            // check if enough parameters:

            if ( conf.get(PropertyNames.INFILE) == null )
                throw new Exception("Input file needed");

            Path fRepo = new Path( conf.get(PropertyNames.REPO_LOCATION) );
            repo = new ToolRepository(FileSystem.get( conf ),fRepo );

            String[] astrToolspecs = repo.getToolList();
            LOG.info( "Available ToolSpecs: ");
            for( String strToolspec: astrToolspecs ) 
                LOG.info( strToolspec );

        } catch (Exception e) {
            printUsage();
            LOG.error(e);
            e.printStackTrace();
            System.exit(-1);
        }
                
        try {
            LOG.info("Running MapReduce ..." );
            res = ToolRunner.run(conf, mr, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(res);
    }       

    /**
     * Prints a usage message for the CLIWrapper.
     */
    public static void printUsage() {
        System.out.println("usage: CLIWrapper -i inFile [-o outFile] [-t mapred.job.reuse.jvm.num.tasks] [-n num lines of inFile per task]");
        System.out.println("    execution of ToolSpec: [-r toolspec repository on hdfs]");
        System.out.println("    execution of Taverna workflow: -w workflow [-v tavernaDir]");
    }
        
}

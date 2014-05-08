package eu.scape_project.pt.mapred;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import eu.scape_project.pt.ToolWrapper;

/**
 * The Toolspec executor.
 *
 * @author Rainer Schmidt [rschmidt13]
 * @author Matthias Rella [myrho]
 */
public class ToolspecMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private final Log LOG = LogFactory.getLog(getClass());
    private ToolWrapper toolWrapper;


    /**
     * Sets up toolspec repository and parser.
     */
    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        /* Moved to ToolWrapper
        String strRepo = conf.get(PropertyNames.REPO_LOCATION);
        Path fRepo = new Path(strRepo);
        FileSystem fs = FileSystem.get(conf);
        this.repo = new ToolRepository(fs, fRepo);

        // create parser of command line input arguments
        parser = new PipedArgsParser();
        */
        this.toolWrapper = new ToolWrapper();
        this.toolWrapper.setup(conf);
    }

    /**
     * The map gets a key and value, the latter being a line describing
     * stdin and stdout file references and (a pipe of) tool/action pair(s) with
     * parameters.
     *
     * 1. Parse the input command-line and read parameters and arguments.
     * 2. Localize input and output file references and input stream.
     * 3. Chain piped commands to a simple-chained list of processors and run them.
     * 4. "De-localize" output files and output streams
     *
     * @param key offset in control file
     * @param value line describing the (piped) command(s) and stdin/out file refs
     * @param context Job context
     */
    @Override
    public void map(LongWritable key, Text value, Context context ) throws IOException {
        LOG.info("Mapper.map key:" + key.toString() + " value:" + value.toString());

        Text text = null;
        try {
            text = new Text(this.toolWrapper.wrap(value.toString()));
        } catch (Exception ex) {
            LOG.error("error during wrapping", ex);
            text = convertToResult(ex);
        } finally {
            writeMappingResult(key, text, context);
        }
    }

    private Text convertToResult(Exception ex) {
        StringWriter writer = new StringWriter();
        ex.printStackTrace(new PrintWriter(writer));
        return new Text( "ERROR: " + writer.toString() );
    }


    private void writeMappingResult(LongWritable key, Text text, Context context) throws IOException {
        try {
            context.write( key, text);
        } catch (InterruptedException ex) {
            LOG.error("error during write", ex);
            throw new IOException(ex);
        }
    }
}

package eu.scape_project.pt.mapred;

import eu.scape_project.pt.ToolWrapper;
import eu.scape_project.pt.proc.Processor;
import eu.scape_project.pt.proc.StreamProcessor;
import eu.scape_project.pt.proc.ToolProcessor;
import eu.scape_project.pt.repo.Repository;
import eu.scape_project.pt.repo.ToolRepository;
import eu.scape_project.pt.tool.Operation;
import eu.scape_project.pt.tool.Tool;
import eu.scape_project.pt.util.CmdLineParser;
import eu.scape_project.pt.util.Command;
import eu.scape_project.pt.util.PipedArgsParser;
import eu.scape_project.pt.util.PropertyNames;
import eu.scape_project.pt.util.fs.Filer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The Toolspec executor.
 *
 * @author Rainer Schmidt [rschmidt13]
 * @author Matthias Rella [myrho]
 */
public class ToolspecMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private final Log LOG = LogFactory.getLog(getClass());
    private static final String SEP = " ";

    private CmdLineParser parser;
    private Repository repo;
    private Tool tool;
    private Operation operation;

    /**
     * Sets up toolspec repository and parser.
     */
    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String strRepo = conf.get(PropertyNames.REPO_LOCATION);
        Path fRepo = new Path(strRepo);
        FileSystem fs = FileSystem.get(conf);
        this.repo = new ToolRepository(fs, fRepo);

        // create parser of command line input arguments
        parser = new PipedArgsParser();
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
            String result = ToolWrapper.wrap(value.toString());
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

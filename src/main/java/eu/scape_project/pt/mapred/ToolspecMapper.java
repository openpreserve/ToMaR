package eu.scape_project.pt.mapred;

import eu.scape_project.pt.proc.Processor;
import eu.scape_project.pt.proc.StreamProcessor;
import eu.scape_project.pt.proc.ToolProcessor;
import eu.scape_project.pt.repo.ToolRepository;
import eu.scape_project.pt.tool.Operation;
import eu.scape_project.pt.tool.Tool;
import eu.scape_project.pt.util.CmdLineParser;
import eu.scape_project.pt.util.Command;
import eu.scape_project.pt.util.PipedArgsParser;
import eu.scape_project.pt.util.PropertyNames;
import eu.scape_project.pt.util.fs.Filer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

    private static Log LOG = LogFactory.getLog(ToolspecMapper.class);

    private CmdLineParser parser;
    private ToolRepository repo;
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
     * @param key TODO what type?
     * @param value line describing the (piped) command(s) and stdin/out file refs
     * @param context Job context
     */
    @Override
    public void map(LongWritable key, Text value, Context context ) throws IOException {
        LOG.info("Mapper.map key:" + key.toString() + " value:" + value.toString());

        String sep = " ";
        Text text = null;
        try {
            // parse input line for stdin/out file refs and tool/action commands
            parser.parse(value.toString());

            final Command[] commands = parser.getCommands();
            final String strStdinFile = parser.getStdinFile();
            final String strStdoutFile = parser.getStdoutFile();

            ToolProcessor firstProcessor = null;
            ToolProcessor lastProcessor = null; 

            Map<String, String>[] mapInputFileParameters = new HashMap[commands.length];
            Map<String, String>[] mapOutputFileParameters = new HashMap[commands.length];

            String directory = System.getProperty("user.dir");
            for(int c = 0; c < commands.length; c++ ) {
                Command command = commands[c];

                this.tool = repo.getTool(command.getTool());

                lastProcessor = new ToolProcessor(this.tool);

                this.operation = lastProcessor.findOperation(command.getAction());
                if( this.operation == null )
                    throw new IOException(
                            "operation " + command.getAction() + " not found");

                lastProcessor.setOperation(this.operation);

                lastProcessor.initialize();

                lastProcessor.setParameters(command.getPairs());

                // get parameters accepted by the lastProcessor.
                mapInputFileParameters[c] = lastProcessor.getInputFileParameters(); 
                mapOutputFileParameters[c] = lastProcessor.getOutputFileParameters(); 

                // copy parameters to temporal map
                Map<String, String> mapTempInputFileParameters = 
                    new HashMap<String, String>(mapInputFileParameters[c]);
                Map<String, String> mapTempOutputFileParameters = 
                    new HashMap<String, String>(mapOutputFileParameters[c]);

                // localize parameters
                for( Entry<String, String> entry : mapInputFileParameters[c].entrySet()) {
                    LOG.debug("input = " + entry.getValue());
                    String[] remoteFileRefs = entry.getValue().split(sep);
                    String localFileRefs = "";
                    for( int i = 0; i < remoteFileRefs.length; i++ ){
                        Filer filer = Filer.create(remoteFileRefs[i]);
                        filer.setDirectory(directory);
                        filer.localize();
                        localFileRefs = localFileRefs + sep + filer.getRelativeFileRef();
                    }
                    mapTempInputFileParameters.put( entry.getKey(), localFileRefs.substring(1));
                }

                for( Entry<String, String> entry : mapOutputFileParameters[c].entrySet()) {
                    LOG.debug("output = " + entry.getValue());
                    String[] remoteFileRefs = entry.getValue().split(sep);
                    String localFileRefs = "";
                    for( int i = 0; i < remoteFileRefs.length; i++ ){
                        Filer filer = Filer.create(entry.getValue());
                        filer.setDirectory(directory);
                        filer.localize();
                        localFileRefs = localFileRefs + sep + filer.getRelativeFileRef();
                    }
                    mapTempOutputFileParameters.put( entry.getKey(), localFileRefs.substring(1));
                }

                // feed processor with localized parameters
                lastProcessor.setInputFileParameters(mapTempInputFileParameters);
                lastProcessor.setOutputFileParameters(mapTempOutputFileParameters);

                // chain processor
                if(firstProcessor == null )
                    firstProcessor = lastProcessor;
                else {
                    Processor help = firstProcessor;  
                    while(help.next() != null ) help = help.next();
                    help.next(lastProcessor);
                }
            }

            // Processors for stdin and stdout
            StreamProcessor streamProcessorIn = null;
            if( strStdinFile != null ) {
                InputStream iStdin = Filer.create(strStdinFile).getInputStream();
                streamProcessorIn = new StreamProcessor(iStdin);
            }

            OutputStream oStdout = null;
            if( strStdoutFile != null ) 
                oStdout = Filer.create(strStdoutFile).getOutputStream();
            else // default: output to bytestream
                oStdout = new ByteArrayOutputStream();

            StreamProcessor streamProcessorOut = new StreamProcessor(oStdout);
            lastProcessor.next(streamProcessorOut);

            if( streamProcessorIn != null ) {
                streamProcessorIn.next(firstProcessor);
                streamProcessorIn.execute();
            } else
                firstProcessor.execute();


            // delocalize output parameters
            for(int i = 0; i < mapOutputFileParameters.length; i++ ) 
                for( String strFile : mapOutputFileParameters[i].values())
                {
                    String[] localFileRefs = strFile.split(sep);
                    for( int j = 0; j < localFileRefs.length; j++ ){
                        Filer filer = Filer.create(localFileRefs[j]);
                        filer.setDirectory(directory);
                        filer.delocalize();
                    }
                }

                if( oStdout instanceof ByteArrayOutputStream )
                    text = new Text( ((ByteArrayOutputStream)oStdout).toByteArray() );
                else
                    text = new Text( strStdoutFile );

        } catch (Exception ex) {
            LOG.error(ex);
            text = new Text( "ERROR: " + ex.getMessage() );
        } finally {
            try {
                context.write( key, text);
            } catch (InterruptedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
}

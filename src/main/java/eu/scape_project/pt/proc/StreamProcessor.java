package eu.scape_project.pt.proc;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

public class StreamProcessor extends Processor {

    private static Log LOG = LogFactory.getLog(StreamProcessor.class);
    private Thread t;

    /**
     * Creates a StreamProcessor that functions as a reader for the 
     * given InputStream.
     * 
     * @param iIn iStdOut 
     */
    public StreamProcessor(InputStream iIn) {
        this.iStdOut = iIn;
        this.oStdIn = null;
    }

    /**
     * Creates a StreamProcessor that functions as a writer for the 
     * given OutputStream.
     * 
     * @param osOut oStdIn 
     */
    public StreamProcessor(OutputStream osOut) {
        this.oStdIn = osOut;
        this.iStdOut = null;
    }

    /**
     * Reads a previous processor's stream and writes to it's own or
     * simply executes the next processor. 
     */
    @Override
    public int execute() throws Exception {
        debugToken = 'S';
        LOG.debug("execute");
        t = new Thread(this);
        t.start();
        if( this.next != null )
        {
            //this.next.setStdIn(oStdIn);
            return this.next.execute();
        }
        return this.waitFor();
    }

    @Override
    public void initialize() {
    }

    /**
     * Waits for the previous processor to terminate.
     */
    @Override
    public int waitFor() throws InterruptedException {
    	if( this.prev == null ) return 0;        
        LOG.debug("waitFor");
    	int r = this.prev.waitFor();
    	if(r < 0) {
    		LOG.info("Terminating stream processor");
        	t.interrupt();
        	return -1;
    	}
    	long timeout = EXECUTION_TIMEOUT_MINUTES*60*1000;
    	long start = System.currentTimeMillis();
    	t.join(timeout);
    	long stop = System.currentTimeMillis();
        if((stop - start) >= timeout) {
        	LOG.warn("Stream execution has reached timeout of "+ EXECUTION_TIMEOUT_MINUTES+" minutes. The process has been terminated!");
        	t.interrupt();
        	return -1;
        }
        return 0;
    }

}

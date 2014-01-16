package eu.scape_project.pt.proc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Processor implements Runnable {

    private static Log LOG = LogFactory.getLog(ToolProcessor.class);
    
    protected char debugToken = 'P';
    /**
     * Inputstream to read from. 
     */
    protected InputStream iStdOut;

    /**
     * Outputstream to write to. 
     */
    protected OutputStream oStdIn;

    /**
     * Processor to execute.
     */
    protected Processor next;

    /**
     * Processor to read output from.
     */
    protected Processor prev;

    protected boolean STOP = false;

    /**
     * Executes its process and provides the InputStream for the next processor.
     * @return exit code of process (0 for success)
     */
	public abstract int execute() throws Exception;
	
	public abstract void initialize();

    /**
     * Gets standard output stream of processor
     * @return InputStream iStdOut
     */
    public InputStream getStdOut( ) {
        return this.iStdOut;
    }

    /**
     * Gets standard input stream of processor
     * @return OutputStream oStdIn
     */
    public OutputStream getStdIn( ) {
        return this.oStdIn;
    }

    /**
     * Sets standard output stream of processor
     */
    public void setStdOut(InputStream out) {
        this.iStdOut = out;
    }

    /**
     * Sets standard input stream of processor
     */
    public void setStdIn(OutputStream in) {
        this.oStdIn = in;
    }

    /**
     * Get next processor
     */
    public Processor next() {
        return next;
    }
    
    /**
     * Get previous processor
     */
    public Processor prev() {
        return prev;
    }

    /**
     * Double-link this processor to given next processor
     */
    public void next(Processor nextProcessor) {
        if( this.next == nextProcessor ) return;
        this.next = nextProcessor;
        nextProcessor.prev(this);
    }

    /**
     * Double-link this processor to given previous processor
     */
    private void prev(Processor prevProcessor) {
        if( this.prev == prevProcessor ) return;
        this.prev = prevProcessor;
        prevProcessor.next(this);
    }

    @Override
    public void run() {
        LOG.debug(debugToken + " run");
        if( this.prev == null 
            || this.prev.getStdOut() == null 
            || oStdIn == null ) return;
        try {
            LOG.debug(debugToken + " copy prev.stdout to stdin");
            LOG.debug("instance of stdout: " + this.prev.getStdOut().getClass().getName() );
            LOG.debug("instance of stdin: " + oStdIn.getClass().getName() );
            IOUtils.copyLarge(this.prev.getStdOut(), oStdIn);
            this.prev.getStdOut().close();
            oStdIn.close();
        } catch (IOException ex) {
            LOG.error("broken pipe");
            throw new RuntimeException(ex);
        }
    }

    public abstract int waitFor() throws InterruptedException;

}

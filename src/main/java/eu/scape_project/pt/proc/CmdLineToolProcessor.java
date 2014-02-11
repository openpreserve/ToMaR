package eu.scape_project.pt.proc;

import eu.scape_project.pt.tool.Operation;
import eu.scape_project.pt.tool.Tool;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Creates processes for a Command Line Tool.
 *
 * @author Matthias Rella [my_rho]
 */
public class CmdLineToolProcessor extends ToolProcessor {

    private static Log LOG = LogFactory.getLog(CmdLineToolProcessor.class);

    /**
     * Underlying sub-process.
     */
    private Process proc;

    public CmdLineToolProcessor(Tool tool) {
        super(tool);
        debugToken = 'C';
    }

    public CmdLineToolProcessor(Tool tool, Operation operation) {
        super(tool, operation);
        debugToken = 'C';
	}

	/**
     * Executes the tool, optionally reading from a previous process (stdin).
     * All input file parameters need to be local to the machine.
     */
    @Override
    public int execute() throws Exception {
        LOG.debug("execute");

        Map<String, String> allInputs = new HashMap<String, String>();
        allInputs.putAll(getInputFileParameters());
        allInputs.putAll(getOutputFileParameters());
        allInputs.putAll(getOtherParameters());

        for (String key : allInputs.keySet()) {
            LOG.debug("Key: " + key + " = " + allInputs.get(key));
        }

        String strCmd = replaceAll(this.operation.getCommand(), allInputs);
        LOG.debug("strCmd = " + strCmd );

        String[] cmd = {"sh", "-c", strCmd};

        proc = Runtime.getRuntime().exec(cmd);

        this.setStdIn(proc.getOutputStream());
        this.setStdOut(proc.getInputStream());

        new Thread(this).start();

        if( this.next != null )
            return this.next.execute();

        return proc.waitFor();
    }

    /** 
     * Waits for the sub-process to terminate.
     */
    @Override
    public int waitFor() throws InterruptedException {
        if( proc == null ) return 0;
        LOG.debug("waitFor");
        return proc.waitFor();
    }

    @Override
    public void initialize() {
    }


}

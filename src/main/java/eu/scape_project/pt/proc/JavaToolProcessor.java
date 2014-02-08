package eu.scape_project.pt.proc;

import eu.scape_project.pt.tool.Operation;
import eu.scape_project.pt.tool.Tool;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Creates processes for a Java Tool.
 *
 * @author Matthias Rella [my_rho]
 */
public class JavaToolProcessor extends ToolProcessor {

    private static Log LOG = LogFactory.getLog(JavaToolProcessor.class);

    /**
     * The main Class of the Java Tool to execute.
     */
    private Class<?> theClass;

    public JavaToolProcessor(Tool tool) {
        super(tool);
        debugToken = 'J';
    }

    public JavaToolProcessor(Tool tool, Operation operation) {
        super(tool, operation);
        debugToken = 'J';
    }

    /**
     * Turns a string into a args-like array.
     * Source: http://stackoverflow.com/questions/366202/regex-for-splitting-a-string-using-space-when-not-surrounded-by-single-or-double
     */
    private String[] toCmdLineArgs(String line) {
        List<String> matchList = new ArrayList<String>();
        Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
        Matcher regexMatcher = regex.matcher(line);
        while (regexMatcher.find()) {
            if (regexMatcher.group(1) != null) {
                // Add double-quoted string without the quotes
                matchList.add(regexMatcher.group(1));
            } else if (regexMatcher.group(2) != null) {
                // Add single-quoted string without the quotes
                matchList.add(regexMatcher.group(2));
            } else {
                // Add unquoted word
                matchList.add(regexMatcher.group());
            }
        }
        return matchList.toArray(new String[0]);
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
        LOG.debug("strCmd = " + strCmd);

        String[] args = parseCmdLineArgs(strCmd);

        securityManager(true);
        this.setStdIn(System.out);
        this.setStdOut(System.in);

        new Thread(this).start();

        int retval = 0;
        if( this.next != null )
            retval = this.next.execute();

        try {
            //TODO needs to execute async
            this.theClass.getMethod("main", String[].class).invoke(null, (Object)args);
        } catch (ExitException ee) {
            LOG.debug("Application tried to exit VM", ee);
        } catch (Exception e) {
            LOG.error("error during class execution " + e);
            throw e;
        }
        securityManager(false);

        return retval;
    }

    /**
     * Parses the command line arguments backwards until
     * there appears a class or a jar file. By the way 
     * returns the arguments found until that class appears.
     *
     * @return String[]
     * @throws IOException
     * @throws ClassNotFoundException
     */

    private String[] parseCmdLineArgs(String strCmd) throws IOException,
            ClassNotFoundException {
        String[] args = toCmdLineArgs(strCmd);

        @SuppressWarnings("rawtypes")
        Class aClass = null;
        ClassNotFoundException ex = new ClassNotFoundException("Java Tool not in classpath");
        Stack<String> sArgs = new Stack<String>();
        for (int i = args.length-1; i > 0; i--) {
            if (args[i].endsWith(".jar")) {
                JarFile jar = new JarFile(args[i], false);
                Manifest manifest = jar.getManifest();
                String mainClass = manifest.getMainAttributes().getValue(
                        "Main-Class");
                jar.close();
                try {
                    aClass = Class.forName(mainClass);
                    break;
                } catch (ClassNotFoundException e) {
                    ex = e;
                }
            } 
            try {
                aClass = Class.forName(args[i]);
                break;
            } catch (ClassNotFoundException e) {
                ex = e;
            }
            sArgs.push(args[i]);
        }
        if( aClass == null ) {
            throw ex;
        }
        this.theClass = aClass;
        return sArgs.toArray(new String[0]);
    }

    @SuppressWarnings("serial")
    protected static class ExitException extends SecurityException {
        public final int status;
        public ExitException(int status)
        {
            super("System.exit() trapped!");
            this.status = status;
        }
    }

    private static class OnExitSecurityManager extends SecurityManager {
        /*
         * * no restrictions here
         * */
        @Override
            public void checkPermission(Permission perm) {
                /*
                 * LOG.info("checking Permission: "+permission.getName());
                 * if(permission.getName().contains("exitVM"))
                 * throw new ExitException("System.exit() called");
                 * */
            }
        /*
         * * no restrictions here
         * */
        @Override
            public void checkPermission(Permission perm, Object context) {}
        /*
         * * Throws Exception if VM is being shut down
         * */
        @Override
            public void checkExit(int status)
            {
                super.checkExit(status);
                throw new ExitException(status);
            }
    }

    private void securityManager(boolean on) {
        SecurityManager securityManager = new OnExitSecurityManager();
        if(on == true) System.setSecurityManager(securityManager);
        else System.setSecurityManager(null);
    }

    @Override
    public void initialize() {
    }

    @Override
    public int waitFor() throws InterruptedException {
        return 0;
    }

}

package eu.scape_project.pt.proc;

import eu.scape_project.pt.tool.Input;
import eu.scape_project.pt.tool.Operation;
import eu.scape_project.pt.tool.Output;
import eu.scape_project.pt.tool.Parameter;
import eu.scape_project.pt.tool.Tool;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Abstract class for Common Toolspec features
 *
 * @author Matthias Rella [my_rho]
 */
abstract public class ToolProcessor extends Processor {

    private static Log LOG = LogFactory.getLog(ToolProcessor.class);

    /**
     * Operation of a Tool to use.
     */
    protected Operation operation;

    /**
     * Tool to use.
     */
    protected Tool tool;

    /**
     * Parameters referring to input files.
     */
    protected Map<String, String> mapInputFileParameters;

    /**
     * Parameters referring to output files.
     */
    protected Map<String, String> mapOutputFileParameters;

    /**
     * Other Parameters. 
     */
    protected Map<String, String> mapOtherParameters;

    /**
     * Constructs the processor with a tool of a Toolspec
     * toolspec.
     */
    public ToolProcessor(Tool tool) {
        this.tool = tool;
        debugToken = 'T';
    }

    /**
     * Constructs the processor with a tool and an operation of a Toolspec
     * toolspec.
     */
    public ToolProcessor(Tool tool, Operation operation) {
        this.tool = tool;
        this.operation = operation;
        debugToken = 'T';
    }


    /**
     * Sets the operation to use for execution. 
     */
    public void setOperation( Operation op ) {
        this.operation = op;
    }

    /**
     * Fills all types of parameters with given Map of parameters.
     */
    public void setParameters( Map<String, String> mapParams) {
        for(Entry<String, String> entry: mapParams.entrySet() ) 
            if( getInputFileParameters().containsKey(entry.getKey()))
                getInputFileParameters().put(entry.getKey(), entry.getValue());
            else if( getOutputFileParameters().containsKey(entry.getKey()))
                getOutputFileParameters().put(entry.getKey(), entry.getValue());
            else if( getOtherParameters().containsKey(entry.getKey()))
                getOtherParameters().put(entry.getKey(), entry.getValue());
    }

    /**
     * Get input file parameters from the toolspec.
     */
    public Map<String, String> getInputFileParameters() {
        LOG.debug("getInputFileParameters");
        if( this.mapInputFileParameters != null )
            return this.mapInputFileParameters;
        Map<String, String> parameters = new HashMap<String, String>();

        if (operation.getInputs() != null) {
            for (Input input : operation.getInputs().getInput()) {
                LOG.debug("input = " + input.getName());
                parameters.put(input.getName(), input.getDefaultValue());
            }
        }
        return this.mapInputFileParameters = parameters;

    }

    /**
     * Get output file parameters from the toolspec.
     */
    public Map<String, String> getOutputFileParameters() {
        LOG.debug("getOutputFileParameters");
        if( this.mapOutputFileParameters != null )
            return this.mapOutputFileParameters;
        Map<String, String> parameters = new HashMap<String, String>();

        if (operation.getOutputs() != null) {
            for (Output output : operation.getOutputs().getOutput()) {
                LOG.debug("output = " + output.getName());
                parameters.put(output.getName(), null);
            }
        }
        return this.mapOutputFileParameters = parameters;
    }

    /**
     * Gets other input parameters from the toolspec.
     */
    public Map<String, String> getOtherParameters() {
        if( this.mapOtherParameters != null )
            return this.mapOtherParameters;
        Map<String, String> parameters = new HashMap<String, String>();

        if (operation.getInputs() != null) {
            for (Parameter param : operation.getInputs().getParameter()) {
                parameters.put(param.getName(), param.getDefaultValue());
            }
        }
        return this.mapOtherParameters = parameters;

    }

    /**
     * Sets input file parameters.
     * 
     * @param mapInput mapTempInputFileParameters 
     */
    public void setInputFileParameters(Map<String, String> mapInput) {
        this.mapInputFileParameters = mapInput;
    }

    /**
     * Sets output file parameters.
     */
    public void setOutputFileParameters(Map<String, String> mapOutput) {
        this.mapOutputFileParameters = mapOutput;
    }

    /**
     * Replaces ${key}s in given command strCmd by values.
     */
    protected String replaceAll(String strCmd, Map<String,String> mapInputs) {
        if( mapInputs.isEmpty() ) return strCmd;
        // create the pattern wrapping the keys with ${} and join them with '|'
        String regexp = "";
        for( String input : mapInputs.keySet())
            regexp += parameterToPlaceholder(input);
        regexp = regexp.substring(0, regexp.length()-1);

        LOG.debug("replaceAll.regexp = " + regexp );
        StringBuffer sb = new StringBuffer();
        Pattern p = Pattern.compile(regexp);
        Matcher m = p.matcher(strCmd);

        while (m.find())
        {
            String param = placeholderToParameter(m.group());
            m.appendReplacement(sb, mapInputs.get(param));
        }
        m.appendTail(sb);

        return sb.toString();

    }

    /**
     * Maps a parameter name to the placeholder's form.
     * Inverse of placeholderToParameter.
     */
    protected String parameterToPlaceholder( String strParameter ) {
        return "\\$\\{" + strParameter + "\\}|"; 
    }

    /**
     * Maps a placeholder to its parameter name.
     * Inverse of parameterToPlaceholder.
     */
    protected String placeholderToParameter( String strVariable ) {
        return strVariable.substring(2, strVariable.length() - 1 );
    }
}

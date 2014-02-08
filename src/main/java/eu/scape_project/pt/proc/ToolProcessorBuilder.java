package eu.scape_project.pt.proc;

import eu.scape_project.pt.tool.Operation;
import eu.scape_project.pt.tool.Operations;
import eu.scape_project.pt.tool.Tool;

/**
 * Build the right ToolProcessor for the given tool and operation.
 *
 * @author Matthias Rella [myrho]
 */

public class ToolProcessorBuilder {

    private Tool tool;
    private Operation operation;

    public void setTool(Tool tool) {
       this.tool = tool; 
    }

    /**
     * Tries to find a operation of the tool.
     * 
     * @param strOp Operation 
     */
    public Operation findOperation( String strOp ) {
        Operations operations = this.tool.getOperations();
        for (Operation op : operations.getOperation()) {
            if (op.getName().equals(strOp)) {
                return op;
            }
        }
        return null;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public ToolProcessor getProcessor() {
        if( this.isJava())
            return new JavaToolProcessor(this.tool, this.operation);
        else
            return new CmdLineToolProcessor(this.tool, this.operation);
    }

    public boolean isJava() {
        return this.operation != null && this.operation.getCommand().contains("java ");
    }
}

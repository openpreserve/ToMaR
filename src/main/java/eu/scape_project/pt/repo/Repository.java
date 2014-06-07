package eu.scape_project.pt.repo;

import eu.scape_project.tool.toolwrapper.data.tool_spec.Tool;

import java.io.IOException;

/**
 * @author Matthias Rella
 */
public interface Repository {

    /**
     * The toolspecs contained in the repository.
     * @return a string array of toolspecs
     */
    String[] getToolList();

    Tool getTool(String tool) throws IOException;

}

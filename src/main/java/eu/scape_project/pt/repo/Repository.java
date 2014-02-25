package eu.scape_project.pt.repo;

import eu.scape_project.pt.tool.Tool;

import java.io.IOException;

/**
 * @author Matthias Rella
 */
public interface Repository {

    /**
     * The toolspecs contained in the repository.
     * @return a string array of toolspecs
     */
    public String[] getToolList();

    public Tool getTool(String tool) throws IOException;

}

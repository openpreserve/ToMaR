package eu.scape_project.pt.repo;

/**
 * @author Matthias Rella
 */
public interface Repository {

    /**
     * The toolspecs contained in the repository.
     * @return a string array of toolspecs
     */
    public String[] getToolList();

}

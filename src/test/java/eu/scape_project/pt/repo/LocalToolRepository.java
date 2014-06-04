package eu.scape_project.pt.repo;

import eu.scape_project.tool.toolwrapper.data.tool_spec.Tool;
import eu.scape_project.tool.toolwrapper.data.tool_spec.utils.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.xml.sax.SAXException;

/**
 * Mock-up repository to use in unit-testing on the local filesystem.
 * 
 * @author Matthias Rella, DME-AIT [myrho]
 */
public class LocalToolRepository implements Repository {
    
    private final File toolsDir;

    /**
     * Construct repository from directory path.
     * @param strToolsDir directory to toolspecs
     */
    public LocalToolRepository(String strToolsDir) throws FileNotFoundException {
        this.toolsDir = new File( strToolsDir );
        if( !toolsDir.isDirectory() ) {
            throw new FileNotFoundException(toolsDir.toString());
        }
    }

    /**
     * Gets Tool from the repository.
     */
    @Override
    public Tool getTool(String toolName ) throws IOException {
        File file = new File( this.toolsDir.getPath() + 
                System.getProperty("file.separator") + toolName + ".xml");

        final FileInputStream fis = new FileInputStream(file);
        try {
            return Utils.fromInputStream( fis );
        } catch (JAXBException ex) {
            throw new IOException(ex);
        } catch (SAXException ex) {
            throw new IOException(ex);
        } finally {
            fis.close();
        }
    }

    @Override
    public String[] getToolList() {
        return this.toolsDir.list();
    }
    
}

package eu.scape_project.pt.repo;

import eu.scape_project.pt.tool.Tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

/**
 * Mock-up repository to use in unit-testing on the local filesystem.
 * 
 * @author Matthias Rella, DME-AIT [myrho]
 */
public class LocalToolRepository implements Repository {
    
	private static JAXBContext jc;
	
	static {
		try {
			jc  = JAXBContext.newInstance(Tool.class);
		} catch (JAXBException e) {
            throw new ExceptionInInitializerError(e);
		}
	}

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

        FileInputStream fis = new FileInputStream(file);
        try {
            return fromInputStream( fis );
        } catch (JAXBException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public String[] getToolList() {
        return this.toolsDir.list();
    }
    
    /**
     * Unmarshals an input stream of xml data to a Tool.
     */
    private Tool fromInputStream(InputStream input) throws JAXBException {
        Unmarshaller u = jc.createUnmarshaller();
        JAXBElement<Tool> unmarshalled = u.unmarshal(new StreamSource(input), Tool.class);
        return unmarshalled.getValue();
    }

}

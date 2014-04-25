package eu.scape_project.pt.repo;

import eu.scape_project.pt.tool.Tool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import eu.scape_project.pt.util.PropertyNames;

/**
 * Manages toolspecs for a given HDFS directory.
 * 
 * @author Matthias Rella [myrho]
 */
public class ToolRepository implements Repository {

	private static Log LOG = LogFactory.getLog(ToolRepository.class);
	private static JAXBContext jc;
	
	static {
		try {
			jc  = JAXBContext.newInstance(Tool.class);
		} catch (JAXBException e) {
		    throw new ExceptionInInitializerError(e);
		}
	}

    private final Path repo_dir;
    private final FileSystem fs;
	
    /**
     * Constructs the repository from a given HDFSystem and a directory path.
     */
    public ToolRepository( FileSystem fs, Path directory ) throws IOException {
       /* if( !fs.exists(directory) )
            throw new FileNotFoundException(directory.toString());*/

        /*if( !fs.getFileStatus(directory).isDir() )
            throw new IOException( directory.toString() + "is not a directory");*/

        this.fs = fs;
        this.repo_dir = directory;
    }
    
    public ToolRepository( Configuration conf, Path directory ) throws IOException {
        /* if( !fs.exists(directory) )
             throw new FileNotFoundException(directory.toString());*/

         /*if( !fs.getFileStatus(directory).isDir() )
             throw new IOException( directory.toString() + "is not a directory");*/

    	
        String strRepo = conf.get(PropertyNames.REPO_LOCATION).trim();
        
        Path fRepo = new Path(strRepo);
        
        fs = fRepo.getFileSystem(conf);    	
    	
         //this.fs = fs;
         this.repo_dir = directory;
     }

    /**
     * Gets a certain Tool from the repository.
     * 
     * @param strTool name of the tool to get
     */
    @Override
    public Tool getTool( String strTool ) throws IOException {
        Path file = new Path( 
                repo_dir.toString() + System.getProperty("file.separator") 
                + getToolName( strTool ) );

        
        FSDataInputStream fis = fs.open( file );
        try {
            return fromInputStream( fis );
        } catch (JAXBException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public String[] getToolList() {
        FileStatus[] list = new FileStatus[0];
        try {
            list = fs.listStatus(repo_dir);
        } catch (IOException ex) {
            LOG.error(ex);
        }
        String strList[] = new String[list.length];
        for( int f = 0; f < list.length; f++ )
            strList[f] = list[f].getPath().getName();
        return strList;
    }

    /**
     * Gets the file name of given tool name.
     */
    private String getToolName( String strTool ) {
        return strTool + ".xml";
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

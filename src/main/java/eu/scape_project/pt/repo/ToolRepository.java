package eu.scape_project.pt.repo;

import eu.scape_project.tool.toolwrapper.data.tool_spec.Tool;
import eu.scape_project.tool.toolwrapper.data.tool_spec.utils.Utils;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.xml.sax.SAXException;

/**
 * Manages toolspecs for a given HDFS directory.
 * 
 * @author Matthias Rella [myrho]
 * @author Alastair Duncan []
 */
public class ToolRepository implements Repository {

	private static Log LOG = LogFactory.getLog(ToolRepository.class);

    private final Path repo_dir;
    private final FileSystem fs;
	
    /**
     * Constructs the repository from a given HDFSystem and a directory path.
     */
    public ToolRepository( FileSystem fs, Path directory ) throws IOException {
        this.fs = fs;
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
            return Utils.fromInputStream( fis );
        } catch (JAXBException ex) {
            throw new IOException(ex);
        } catch (SAXException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public String[] getToolList() {
        FileStatus[] list = new FileStatus[0];
        try {
            list = fs.listStatus(repo_dir);
        } catch (IOException ex) {
            LOG.error("", ex);
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

}

package eu.scape_project.pt.util.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Implementing classes of this interface handle the transportation of files and
 * directories from local to remote filesystem and vice-versa. A remote filesystem
 * may be HDFS as for example.
 * 
 * @author Rainer Schmidt [rschmidt13]
 * @author Matthias Rella [myrho]
 */
public abstract class Filer {

    protected String dir = "";

    /**
     * Abstract factory method to create appropriate file for given uri
     */
    public static Filer create(String strUri) throws IOException{
        URI uri = null;
        try {
            uri = new URI(strUri);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
        String scheme = uri.getScheme();
        if( scheme.equals("hdfs")) {
            return new HDFSFiler(uri);
        }else if(scheme.equals("wasb")){
        	return new WASBFiler(uri);
        }
        throw new IOException("no appropriate filer for URI " + strUri + " found");
    }

    public String getTmpDir() {
        return System.getProperty("java.io.tmpdir") 
                + System.getProperty("file.separator");
    }
    
    /**
     * Sets the working directory where to localize remote files or directories to.
     */
    public abstract void setWorkingDir(String strDir ) throws IOException;

    /**
     * Copies file or directory to local filesystem.
     */
    public abstract void localize() throws IOException;

    /**
     * Copies file or directory from local filesystem to remote one.
     */
    public abstract void delocalize() throws IOException;

    /**
     * Gets the absolute local file reference of the filer's file.
     * @return String fileRef
     */
    public abstract String getAbsoluteFileRef();

    /**
     * Gets the relative local file reference of the filer's file.
     * @return String fileRef
     */
    public abstract String getRelativeFileRef();

    /**
     * Gets the input stream of a file.
     */
    public abstract InputStream getInputStream() throws IOException;

    /**
     * Gets the output stream to a file.
     */
    public abstract OutputStream getOutputStream() throws IOException;

}

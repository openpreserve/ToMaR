package eu.scape_project.pt.util.fs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
        if( strUri.startsWith("hdfs")) {
            return new HDFSFiler(strUri.toString());
        }
        throw new IOException("no appropriate filer for URI " + strUri + " found");
    }

    public String getTmpDir() {
        return System.getProperty("java.io.tmpdir") 
                + System.getProperty("file.separator");
    }
	
    public abstract void setDirectory(String strDir ) throws IOException;

    /**
     * Copies a file from a remote filesystem to the local one.
     */
	public abstract File copyFile(String strSrc, String strDest) throws IOException;

    /**
     * Copies a file or directory from the local filesystem to a remote one.
     */
	public abstract void depositDirectoryOrFile(String srcSrc, String strDest) throws IOException; 

    /**
     * Copies a directory from the local filesystem to a remote one.
     */
	public abstract void depositDirectory(String strSrc, String strDest) throws IOException;
	
    /**
     * Copies a file from the local filesystem to a remote one.
     */
	public abstract void depositFile(String strSrc, String strDest) throws IOException;

    /**
     * Copies file to local filesystem.
     */
    public abstract void localize() throws IOException;

    /**
     * Copies file from local filesystem to remote one.
     */
    public abstract void delocalize() throws IOException;

    /**
     * Gets the local file reference of the filer's file.
     * @return String fileRef
     */
    public abstract String getFileRef();

    /**
     * Gets the input stream of a file.
     */
    public abstract InputStream getInputStream() throws IOException;

    /**
     * Gets the output stream to a file.
     */
    public abstract OutputStream getOutputStream() throws IOException;

}

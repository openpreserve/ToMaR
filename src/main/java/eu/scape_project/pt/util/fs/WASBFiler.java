package eu.scape_project.pt.util.fs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Handles the transportation of files from the local filesystem to WASB Azure Blob storage 
 * and vice-versa.
 * 
 * @author Alastair Duncan
 * 
 */
public class WASBFiler extends Filer {
    
    private static Log LOG = LogFactory.getLog(WASBFiler.class);
    
    /**
     * Blob Filesystem handle.
     */
    private final FileSystem wasb;

    /**
     * File to handle by this filer
     */
    private final Path file;

    WASBFiler(URI uri) throws IOException {
        this.file = new Path(uri);
        wasb = file.getFileSystem(new Configuration());
    }
    
    @Override
    public void localize() throws IOException {
        File fileRef = new File(getAbsoluteFileRef());
        LOG.debug("localise " + fileRef);
        new File(fileRef.getParent()).mkdirs();
        Path localfile = new Path( fileRef.toString() );
        if(wasb.exists(file)) {
            wasb.copyToLocalFile(file, localfile);
        }
    }

    @Override
    public void delocalize() throws IOException {
        this.depositDirectoryOrFile(getAbsoluteFileRef(), file.toString());
    }

    @Override
    public void setWorkingDir(String strDir ) {
        LOG.debug("setDirectory " + strDir );
        File dirFile = new File(strDir);
        if( !dirFile.isAbsolute() ) {
            this.dir = this.getTmpDir() + strDir;
        } else {
            this.dir = strDir;
        }
        LOG.debug("this.dir = " + this.dir );
    }

    @Override
    public String getAbsoluteFileRef() {
        return this.getFullDirectory();
    }

    @Override
    public String getRelativeFileRef() {
        String path = this.getFullDirectory();
        
        if( path.startsWith(System.getProperty("file.separator")) || path.startsWith("\\") )
            path = path.substring(1);
        return path;
    }
    
    @Override
    public InputStream getInputStream() throws IOException {
        return wasb.open(file);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return wasb.create(file);
    }
    
    /**
     * Returns the user defined directory of the file.
     */
    private String getPath() {
        URI uri = this.file.toUri();
        String path = uri.getPath();
        LOG.debug("path = " + path);
        System.out.println("getPath: path = " + path);
        return path;
    }
    
    /**
     * Copies a file or directory from the local filesystem to a remote one.
     */
    private void depositDirectoryOrFile(String strSrc, String strDest) throws IOException {
        File source = new File( strSrc );
        // no directories in wasb
        // make sure the file separator is correct
        strSrc = "file:///" + strSrc.replace("/", "\\");
        System.out.println("depositDirectoryOrFile: strSrc = " + strSrc);
        depositFile(strSrc, strDest);
    }

    /**
     * Copies a file from the local filesystem to a remote one.
     */
    private void depositFile(String strSrc, String strDest) throws IOException {
    	System.out.println("depositFile: strSrc: " + strSrc + " strDest:" + strDest);
        Path src = new Path(strSrc);
        Path dest = new Path(strDest);
        
        LOG.debug("depositFile: src: "+src+" dest: " +dest);
        System.out.println("depositFile: src: " + src + " dest: " + dest);
        
        wasb.copyFromLocalFile(src, dest);
    }
    
    /**
     * Returns working space directory with user defined directories.
     */
    private String getFullDirectory() {
        String par = this.getPath();
        par = par.substring(par.lastIndexOf("/"));
        String result = "";
        if(this.dir.isEmpty()){
        	result = "wasbfiler_" + file.hashCode();
        }else{
        	result = this.dir;
        }
        
        return result + par;
    }
}
    
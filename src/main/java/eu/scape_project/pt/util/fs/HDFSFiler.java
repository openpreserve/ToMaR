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
 * Handles the transportation of files from the local filesystem to HDFS 
 * and vice-versa.
 * 
 * @author Rainer Schmidt [rschmidt13]
 * @author Matthias Rella [myrho]
 * @author Martin Schenck [schenck]
 */
public class HDFSFiler extends Filer {
	
	private static Log LOG = LogFactory.getLog(HDFSFiler.class);
	
    /**
     * Hadoop Filesystem handle.
     */
	private final FileSystem hdfs;

    /**
     * File to handle by this filer
     */
    private final Path file;

    HDFSFiler(URI uri) throws IOException {
        this.file = new Path(uri);
        hdfs = file.getFileSystem(new Configuration());
    }
	
    @Override
	public File copyFile(String strSrc, String strDest) throws IOException {
		Path path = new Path(strSrc);
		if(!hdfs.exists(path)) throw new IOException("file does not exist! "+strSrc);
		//File temp = File.createTempFile(path.getName(), "", tempDir);		
        File temp = new File( strDest );
		hdfs.copyToLocalFile(path, new Path(strDest));
		return temp;
	}
	
	@Override
	public void depositDirectoryOrFile(String strSrc, String strDest) throws IOException {
        File source = new File( strSrc );
		if(source.isDirectory()) {
			depositDirectory(strSrc, strDest);
		} else {
			depositFile(strSrc, strDest);
		}
	}
	
	@Override
	public void depositDirectory(String strSrc, String strDest) throws IOException {
		// Get output directory name from strSrc
        File localDir = new File( strSrc );
		
		if(!localDir.isDirectory()) {
			throw new IOException("Could not find correct local output directory: " + localDir );
		}
		
		LOG.info("Local directory is: " + localDir );
		
        // FIXME if strSrc is a directory then strDest should be a directory too
		for(File localFile : localDir.listFiles()) {
			depositDirectoryOrFile(localFile.getCanonicalPath(), strDest + File.separator + localFile.getName());
		}
	}

	@Override
	public void depositFile(String strSrc, String strDest) throws IOException {
		Path src = new Path(strSrc);
		Path dest = new Path(strDest);
		
		LOG.info("local file name is: "+src+" destination path is:" +dest);
		hdfs.copyFromLocalFile(src, dest);
	}

    @Override
    public void localize() throws IOException {
        Path localfile = new Path( getFileRef() );
        if(hdfs.exists(file)) {
            hdfs.copyToLocalFile(file, localfile);
        }
    }

    @Override
    public void delocalize() throws IOException {
        Path localfile = new Path( getFileRef() );
        //hdfs.copyFromLocalFile(localfile, file);
        this.depositDirectoryOrFile(getFileRef(), file.toString());
        // TODO: if output ref is a directory, the directory exists as a file in the temp locaiton. this directory is copied as is to HDFS
        // eg. --output="/some/dir/data" will result in /some/dir/data/data on HDFS if /some/dir/data already exists on HDFS. 
        // we should circumvent this, by checking if a directory exists. and we also need to create the directory on the local system if it exists on HDFS already (in localize)
    }

    @Override
    public void setDirectory(String strDir ) {
        File tempDir = new File(getTmpDir() + strDir );
        tempDir.mkdir();
        this.dir = strDir;
    }

    @Override
    public String getFileRef() {
        // TODO introduce a namespace for temp files so that
        // other running tasks on the machine don't interfere
        return getTmpDir()
                    + (this.dir.isEmpty() 
                            ? "hdfsfiler_" + file.hashCode() + "-" 
                            : this.dir + System.getProperty("file.separator")) 
                    + file.getName();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return hdfs.open(file);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return hdfs.create(file);
    }

}

package eu.scape_project.pt.mapred.input;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

@SuppressWarnings("unused")
public class MockupFileSystem extends FileSystem {
    private static final Log LOG = LogFactory.getLog(MockupFileSystem.class);

    class MockupFile {
        private boolean exists;
        private BlockLocation[] locations;
        private ByteArrayOutputStream out;

        public MockupFile(String filename, boolean exists,
                BlockLocation[] locations) {
            this.exists = exists;
            this.locations = locations;
        }

        public boolean exists() {
            return exists;
        }

        public BlockLocation[] getLocations() {
            return locations;
        }

        public void setOutputStream(ByteArrayOutputStream out) {
            this.out = out;
        }

        public ByteArrayInputStream getInputStream() {
            if( this.out == null ) return null;
            byte[] output = this.out.toByteArray();
            LOG.debug("output = " + new String(output, Charset.defaultCharset()));
            return new ByteArrayInputStream(output);    
        }

    }

    private HashMap<String, MockupFile> mockupFiles = new HashMap<String, MockupFile>();

    @Override
    public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) {
        return null;
    }

    @Override
    public FSDataOutputStream create(Path arg0) throws IOException {
        LOG.debug("create output stream for " + arg0.toString());
        if (!exists(arg0)) {
            throw new IOException(arg0 + " does not exist");
        }
        Path p = new Path(System.getProperty("java.io.tmpdir") + File.separator + arg0.toString());
        LOG.debug("path = " + p.toString());
        
        FileSystem fs = FileSystem.get(new Configuration());
        fs.createNewFile(p);
        return fs.create(p, true);
    }

    @Override
    public FSDataOutputStream create(Path arg0, FsPermission arg1,
            boolean arg2, int arg3, short arg4, long arg5, Progressable arg6)
            throws IOException {
        return create(arg0);
    }

    @Override
    public boolean delete(Path arg0) {
        return false;
    }

    @Override
    public boolean delete(Path arg0, boolean arg1) {
        return false;
    }

    @Override
    public FileStatus getFileStatus(Path arg0) {
        return new FileStatus(0, false, 0, 0, 0, arg0);
    }

    @Override
    public URI getUri() {
        return null;
    }

    @Override
    public Path getWorkingDirectory() {
        return null;
    }

    @Override
    public FileStatus[] listStatus(Path arg0) {
        return null;
    }

    @Override
    public boolean mkdirs(Path arg0, FsPermission arg1) {
        return false;
    }

    @Override
    public FSDataInputStream open(Path arg0) throws IOException {
        LOG.debug("create input stream for " + arg0.toString());
        if (!exists(arg0)) {
            throw new IOException(arg0 + " does not exist");
        }

        Path p = new Path(System.getProperty("java.io.tmpdir") + File.separator + arg0.toString());

        FileSystem fs = FileSystem.get(new Configuration());
        return fs.open(p);
    }

    @Override
    public FSDataInputStream open(Path arg0, int arg1) throws IOException {
        return open(arg0);
    }

    public ByteArrayInputStream getContent(Path arg0) throws IOException {
        LOG.debug("create input stream for " + arg0.toString());
        if (!exists(arg0)) {
            throw new IOException(arg0 + " does not exist");
        }
        MockupFile file = mockupFiles.get(arg0.toString());
        return file.getInputStream();
    }

    @Override
    public boolean rename(Path arg0, Path arg1) {
        return false;
    }

    @Override
    public void setWorkingDirectory(Path arg0) {

    }

    public void addFile(String filename, boolean exists, BlockLocation[] locations) {
        LOG.debug("addFile " + filename);
        mockupFiles.put(filename, new MockupFile(filename, exists, locations));
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus status, long start, long end) {
        return mockupFiles.get(status.getPath().toString()).getLocations();
    }

    @Override
    public boolean exists(Path file) {
        return mockupFiles.containsKey(file.toString()) && mockupFiles.get(file.toString()).exists();
    }
}

package eu.scape_project.pt.mapred.input;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class MockupFileSystem extends FileSystem {

    class MockupFile {
        private String filename;
        private boolean exists;
        private BlockLocation[] locations;

        public MockupFile(String filename, boolean exists, BlockLocation[] locations) {
            this.filename = filename;
            this.exists = exists;
            this.locations = locations;
        }

        public boolean exists() {
            return exists;
        }

        public BlockLocation[] getLocations() {
            return locations;
        }

    }

    private HashMap<String, MockupFile> mockupFiles = new HashMap<String, MockupFile>();

    public MockupFileSystem() {
    };

    @Override
    public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FSDataOutputStream create(Path arg0, FsPermission arg1,
            boolean arg2, int arg3, short arg4, long arg5, Progressable arg6)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean delete(Path arg0) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean delete(Path arg0, boolean arg1) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public FileStatus getFileStatus(Path arg0) throws IOException {
        return new FileStatus(0, false, 0, 0, 0, arg0);
    }

    @Override
    public URI getUri() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path getWorkingDirectory() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FileStatus[] listStatus(Path arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public FSDataInputStream open(Path arg0, int arg1) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean rename(Path arg0, Path arg1) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setWorkingDirectory(Path arg0) {
        // TODO Auto-generated method stub

    }

    public void addFile(String filename, boolean exists,
            BlockLocation[] locations) {
        mockupFiles.put(filename, new MockupFile(filename, exists, locations));
    }

    public BlockLocation[] getFileBlockLocations(FileStatus status, long start,
            long end) {
        return mockupFiles.get(status.getPath().toString()).getLocations();
    }

    public boolean exists(Path file) {
        return mockupFiles.get(file.toString()).exists();
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.scape_project.pt.mapred.input;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.LineReader;

import eu.scape_project.pt.proc.ToolProcessor;
import eu.scape_project.pt.repo.Repository;
import eu.scape_project.pt.repo.ToolRepository;
import eu.scape_project.tool.toolwrapper.data.tool_spec.Operation;
import eu.scape_project.tool.toolwrapper.data.tool_spec.Tool;
import eu.scape_project.pt.util.CmdLineParser;
import eu.scape_project.pt.util.Command;
import eu.scape_project.pt.util.PipedArgsParser;
import eu.scape_project.pt.util.PropertyNames;

/**
 * ControlFileInputFormat for a control file with references to files on HDFS.
 *
 * Each line of a control file contains descriptions of toolspec/operation invocations
 * with additional references to files on HDFS. For instance, given a toolspec
 * "hash" with the operation "md5" a control line might look like this:
 *
 * hash md5 --input="hdfs://path/to/file"
 *
 * Hadoop's NLineInputFormat would split the control file so that each map task
 * gets N such lines. However, as NLineInputFormat is agnostic of the block locations of 
 * the input file reference it will spill the map task  to any node arbitrarily.
 *
 * ControlFileInputFormat extends NLineInputformat and adds logic for 
 * <ol>
 * <li>resolving a control line's toolspec and input file descriptors contained
 * <li>retrieving and mapping of block locations to control lines
 * <li>reordering and balancing control lines by their block locations
 * <li>giving Hadoop location hints where to assign a consecutive bunch of lines to
 * </ol>
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ControlFileInputFormat extends NLineInputFormat {
    private static Log LOG = LogFactory.getLog(ControlFileInputFormat.class);

    /** 
     * Logically splits the set of input files for the job, splits N lines
     * of the input as one split.
     * 
     * @see NLineInputFormat#getSplits(JobContext)
     */
    @Override
    public List<InputSplit> getSplits(JobContext job)
        throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        int numLinesPerSplit = getNumLinesPerSplit(job);
        for (FileStatus status : listStatus(job)) {
            splits.addAll(getSplitsForFile(status,
                        job.getConfiguration(), numLinesPerSplit));
        }
        return splits;
    }

    /**
     * Gets the rearranged splits for a control file.
     *
     * Rearranges the lines of a control file according to the location
     * the input file references and logically splits the rearranged control file
     * into splits of about N lines.
     */
    public static List<FileSplit> getSplitsForFile(FileStatus status,
            Configuration conf, int numLinesPerSplit) throws IOException {
        List<FileSplit> splits = new ArrayList<FileSplit>();
        Path controlFile = status.getPath();
        if (status.isDirectory()) {
            throw new IOException("Not a file: " + controlFile);
        }
        FileSystem fs = controlFile.getFileSystem(conf);
        CmdLineParser parser = new PipedArgsParser();
        String strRepo = conf.get(PropertyNames.REPO_LOCATION);
        Path fRepo = new Path(strRepo);
        Repository repo = new ToolRepository(fs, fRepo);

        LOG.info("Creating location-aware control file");
        Map<String, ArrayList<String>> locationMap = createLocationMap(controlFile, conf, repo, parser);

        Path newControlFile = new Path(controlFile + "-rearranged"
                + System.currentTimeMillis());

        splits = writeNewControlFileAndCreateSplits(newControlFile, fs, locationMap,
                numLinesPerSplit);
        LOG.info("Location-aware control file " + newControlFile.toString() + " created");
        return splits;
    }

    /**
     * Loops over locationMap, writes control lines from locationmap
     * to new control file and creates equally sized splits of
     * approximate numLinesPerSplit.
     *
     * @param newControlFile Path to the reordered control file
     * @param fs Hadoop filesystem handle
     * @param locationMap map of control line -> arraylist of locations
     * @param numLinesPerSplit approximate number of lines per split
     * @return list of splits
     */
    public static List<FileSplit> writeNewControlFileAndCreateSplits(
            Path newControlFile, FileSystem fs,
            Map<String, ArrayList<String>> locationMap, int numLinesPerSplit)
            throws IOException {
        int start = 0, byteCounter = 0;
        List<FileSplit> splits = new ArrayList<FileSplit>();
        FSDataOutputStream fsout = fs.create(newControlFile);
        for (Entry<String, ArrayList<String>> entry : locationMap.entrySet()) {
            String host = entry.getKey();

            ArrayList<String> lines = entry.getValue();
            int numSplits = lines.size() / numLinesPerSplit;
            int rest = lines.size() % numLinesPerSplit;

            if (lines.size() <= numLinesPerSplit) {
                //   put all lines into newControlFile and use the whole as a split
                for (String line : lines) {
                    fsout.write((line+"\n").getBytes()); 
                    byteCounter += line.length()+1;
                }
                // create the split and provide a location hint
                splits.add(new FileSplit(newControlFile, start,
                        byteCounter-start, new String[] { host }));
                start = byteCounter;
            } else {
                int i = 1;
                int j = rest;
                for (String line : lines) {
                    fsout.write((line+"\n").getBytes());
                    byteCounter += line.length()+1;
                    if (i < numLinesPerSplit + (j/numSplits)) {
                        i++;
                    } else {
                        // create the split and provide a location hint
                        splits.add(new FileSplit(newControlFile, start,
                                (byteCounter-start), new String[] { host }));
                        start = byteCounter;
                        if ( j >= numSplits ) {
                            j = numSplits % rest;
                        }
                        j += rest;
                        i = 1;
                    }
                }
            }
        }
        fsout.close();
        return splits;
    }

    /**
     * Creates mapping of locations to arraylists of control lines.
     *
     * @param controlFile input control file
     * @param conf Hadoop configuration
     * @param repo Toolspec repository
     * @param parser parser for the control lines
     * @return mapping
     */
    public static Map<String, ArrayList<String>> createLocationMap(
            Path controlFile, Configuration conf, Repository repo,
            CmdLineParser parser) throws IOException {
        FileSystem fs = controlFile.getFileSystem(conf);
        FSDataInputStream in = fs.open(controlFile);
        LineReader lr = new LineReader(in, conf);
        Text controlLine = new Text();
        Map<String, ArrayList<String>> locationMap = new HashMap<String, ArrayList<String>>();
        ArrayList<String> allHosts = new ArrayList<String>();
        int l = 0;
        while ((lr.readLine(controlLine)) > 0) {
            l += 1;
            // read line by line
            Path[] inFiles = getInputFiles(fs, parser, repo, controlLine.toString());

            // count for each host how many blocks it holds of the current control line's input files
            String[] hostsOfFile = getSortedHosts(fs, inFiles);

            for ( String host : hostsOfFile ) {
                if( !allHosts.contains(host)) allHosts.add(host);
            }
            ArrayList<String> theseHosts = (ArrayList<String>)allHosts.clone();
            for ( String host : hostsOfFile ) {
                theseHosts.remove(host);
            }

            String[] hosts = new String[theseHosts.size() + hostsOfFile.length];
            int h = 0;
            for ( String host : hostsOfFile ) {
                hosts[h] = hostsOfFile[h];
                h++;
            }
            for( String host : theseHosts ) {
                hosts[h] = theseHosts.get(h-hostsOfFile.length);
                h++;
            }
            
            addToLocationMap(locationMap, hosts, controlLine.toString(), l);
        }
        return locationMap;
    }

    /**
     * Adds control line to the locationmap and keeps locations balanced.
     *
     * @param locationMap mapping of locations to arraylists of control lines
     * @param hosts locations of the control line
     * @param line control line
     * @param lineNum current line number of original control file
     */
    public static void addToLocationMap(
        Map<String, ArrayList<String>> locationMap, String[] hosts,
        String line, int lineNum) {
        for (String host : hosts) {
            ArrayList<String> lines = locationMap.containsKey(host) ? locationMap
                    .get(host) : new ArrayList<String>();
            // if the location is unbalanced (got too few lines) add line for that location
            if (lines.size() < (float)lineNum / hosts.length ) {
                lines.add(line);
                locationMap.put(host, lines);
                break;
            }
        }
    }

    /**
     * Finds input file references in the control line by looking 
     * into its toolspec.
     *
     * @param fs Hadoop filesystem handle
     * @param parser for parsing the control line
     * @param repo Toolspec repository
     * @return array of paths to input file references
     */
    public static Path[] getInputFiles(FileSystem fs, CmdLineParser parser,
            Repository repo, String controlLine) throws IOException {
        parser.parse(controlLine);

        Command command = parser.getCommands()[0];
        String strStdinFile = parser.getStdinFile();
        // parse it, read input file parameters
        Tool tool = repo.getTool(command.getTool());

        ToolProcessor proc = new ToolProcessor(tool);
        Operation operation = proc.findOperation(command.getAction());
        if (operation == null)
            throw new IOException("operation " + command.getAction()
                    + " not found");

        proc.setOperation(operation);
        proc.setParameters(command.getPairs());
        Map<String, String> mapInputFileParameters = proc
                .getInputFileParameters();
        ArrayList<Path> inFiles = new ArrayList<Path>();
        if (strStdinFile != null) {
            Path p = new Path(strStdinFile);
            if (fs.exists(p)) {
                inFiles.add(p);
            }
        } 

        for (String fileRef : mapInputFileParameters.values()) {
            Path p = new Path(fileRef);
            if (fs.exists(p) ) { 
                if( fs.isDirectory(p) ) {
                    inFiles.addAll(getFilesInDir(fs, p));
                } else {
                    inFiles.add(p);
                }
            }
        }
        return inFiles.toArray(new Path[0]);
    }

    /**
     * Recursively collects paths in a directory.
     *
     * @param fs Hadoop filesystem handle
     * @param path path, a directory
     * @return list of paths
     */
    private static List<Path> getFilesInDir(FileSystem fs, Path path)
            throws FileNotFoundException, IOException {
        ArrayList<Path> inFiles = new ArrayList<Path>();
        for( FileStatus s : fs.listStatus(path) ) {
            if( s.isDirectory() ) {
                inFiles.addAll(getFilesInDir(fs, s.getPath()));
            } else {
                inFiles.add(s.getPath());
            }
        }
        return inFiles;
    }

    /**
     * Gets block locations of input files sorted
     * by the total number of occurrences.
     *
     * @param fs Hadoop filesystem handle
     * @param inFiles array of input files
     * @return sorted String array
     */
    public static String[] getSortedHosts(FileSystem fs, Path[] inFiles)
            throws IOException {
        final Map<String, Integer> hostMap = new HashMap<String, Integer>();
        for( Path inFile : inFiles ) {
            FileStatus s = fs.getFileStatus(inFile);
            BlockLocation[] locations = fs.getFileBlockLocations(s, (long)0, s.getLen());
            for( BlockLocation location : locations ) {
                String[] hosts = location.getHosts();
                for( String host : hosts ) {
                    if( !hostMap.containsKey(host) ) {
                        hostMap.put(host, 1);
                        continue;
                    }
                    hostMap.put(host, hostMap.get(host)+1);
                }
            }
        }
        // sort hosts by number of references to blocks of input files
        List<String> hosts = new ArrayList<String>();
        hosts.addAll(hostMap.keySet());
        Collections.sort(hosts, new Comparator<String>() {
            @Override
            public int compare(String host1, String host2) {
                return hostMap.get(host2) - hostMap.get(host1);
            }
        });
        return hosts.toArray(new String[0]);

    }

}


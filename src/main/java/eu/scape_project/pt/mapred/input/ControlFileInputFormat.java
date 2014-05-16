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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.LineReader;

import eu.scape_project.pt.tool.Operation;
import eu.scape_project.pt.tool.Tool;
import eu.scape_project.pt.util.CmdLineParser;
import eu.scape_project.pt.util.Command;
import eu.scape_project.pt.util.PipedArgsParser;
import eu.scape_project.pt.util.PropertyNames;
import eu.scape_project.pt.proc.ToolProcessor;
import eu.scape_project.pt.repo.Repository;
import eu.scape_project.pt.repo.ToolRepository;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
 * get N such lines. However, as for data locality it is important that the line gets
 * processed by a node which actually hosts the referenced input file. As 
 * NLineInputFormat is agnostic of the input file reference it will spill the map task
 * to any node arbitrarily. 
 *
 * ControlFileInputFormat extends NLineInputformat
 *
 * The NLineInputFormat can be used in such applications, that splits
 * the input file such that by default, one line is fed as
 * a value to one map task, and key is the offset.
 * i.e. (k,v) is (LongWritable, Text).
 * The location hints will span the whole mapred cluster.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ControlFileInputFormat extends NLineInputFormat {
    private static Log LOG = LogFactory.getLog(ControlFileInputFormat.class);
    public static final String LINES_PER_MAP = "mapreduce.input.lineinputformat.linespermap";

    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit genericSplit, TaskAttemptContext context)
            throws IOException {
        context.setStatus(genericSplit.toString());
        return new LineRecordReader();
    }

    /**
     * Rearranges the lines of the control files according to the location
     * the input file references and logically splits the rearranged control file 
     * into splits of about N lines.
     *
     * @see NLineInputFormat#getSplits(JobContext)
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        int numLinesPerSplit = getNumLinesPerSplit(job);
        for (FileStatus status : listStatus(job)) {
            splits.addAll(getSplitsForFile(status, job.getConfiguration(),
                    numLinesPerSplit));
        }
        return splits;
    }

    public static List<FileSplit> getSplitsForFile(FileStatus status,
            Configuration conf, int numLinesPerSplit) throws IOException {
        List<FileSplit> splits = new ArrayList<FileSplit>();
        Path fileName = status.getPath();
        LOG.debug("fileName = " + fileName.toString());
        if (status.isDir()) {
            throw new IOException("Not a file: " + fileName);
        }
        FileSystem fs = fileName.getFileSystem(conf);
        LineReader lr = null;
        try {
            CmdLineParser parser = new PipedArgsParser();
            String strRepo = conf.get(PropertyNames.REPO_LOCATION);
            Path fRepo = new Path(strRepo);
            Repository repo = new ToolRepository(fs, fRepo);

            Map<String, ArrayList<String>> locationMap = createLocationMap(fileName, conf, repo, parser);

            Path newControlFile = new Path(fileName + "-rearranged"
                    + System.currentTimeMillis());
            LOG.debug("newControlFile = " + newControlFile.toString());

            splits = writeNewControlFileAndCreateSplits(newControlFile, fs, locationMap,
                    numLinesPerSplit);

        } finally {
            if (lr != null) {
                lr.close();
            }
        }
        return splits;
    }

    public static List<FileSplit> writeNewControlFileAndCreateSplits(
            Path newControlFile, FileSystem fs,
            Map<String, ArrayList<String>> locationMap, int numLinesPerSplit)
            throws IOException {
        int start = 0, byteCounter = 0;
        List<FileSplit> splits = new ArrayList<FileSplit>();
        FSDataOutputStream fsout = fs.create(newControlFile);
        LOG.debug("numLinesPerSplit = " + numLinesPerSplit);
        for (Entry<String, ArrayList<String>> entry : locationMap
                .entrySet()) {
            String host = entry.getKey();
            LOG.debug(" for host = " + host);
            ArrayList<String> lines = entry.getValue();
            int numSplits = lines.size() / numLinesPerSplit;
            int rest = lines.size() % numLinesPerSplit;
            double restPerSplit = rest / (double)numSplits;
            LOG.debug(" numSplits = " + numSplits);
            LOG.debug(" rest = " + rest);
            LOG.debug(" restPerSplit = " + restPerSplit);
            // x = arraysize / N ... number of splits
            // r = x - int(x) ... fraction of rest
            // b = (int)(N*r) ... number of rest
            // a = b/int(x) ... number of lines per split to add
            // i = 0; j = 0; start = 0

            // if arraysize < N
            if (lines.size() <= numLinesPerSplit) {
                LOG.debug("arraysize < N ");
                //   put all lines into newControlFile and use the whole as a split
                LOG.debug("writing all lines to newControlFile");
                for (String line : lines) {
                    LOG.debug("line = " + line);
                    fsout.write((line+"\n").getBytes()); // FIXME : add a line break?
                    byteCounter += line.length()+1;
                }
                splits.add(new FileSplit(newControlFile, start,
                        byteCounter-start, new String[] { host }));
                LOG.debug("split created, start = " + start + ", end = "
                        + (byteCounter-start) + ", host = " + host);
                start = byteCounter;
            } else {
                LOG.debug("arraysize > N");
                int i = 1;
                int j = rest;
                for (String line : lines) {
                    LOG.debug("i = " + i + ", j = " + j );
                    LOG.debug("writing line to newControlFile");
                    LOG.debug("line = " + line);
                    fsout.write((line+"\n").getBytes());
                    byteCounter += line.length()+1;
                    if (i < numLinesPerSplit + (j/numSplits)) {
                        i++;
                    } else {
                        splits.add(new FileSplit(newControlFile, start,
                                (byteCounter-start), new String[] { host }));
                        LOG.debug("split created, start = " + start
                                + ", length = " + (byteCounter-start) + ", host = "
                                + host);
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

    public static Map<String, ArrayList<String>> createLocationMap(
            Path fileName, Configuration conf, Repository repo,
            CmdLineParser parser) throws IOException {
        FileSystem fs = fileName.getFileSystem(conf);
        FSDataInputStream in = fs.open(fileName);
        LineReader lr = new LineReader(in, conf);
        Text controlLine = new Text();
        Map<String, ArrayList<String>> locationMap = new HashMap<String, ArrayList<String>>();
        ArrayList<String> allHosts = new ArrayList<String>();
        int l = 0;
        while ((lr.readLine(controlLine)) > 0) {
            l += 1;
            LOG.debug("l = " + l);
            // read line by line
            Path[] inFiles = getInputFiles(fs, parser, repo, controlLine.toString());

            // count for each host how many blocks it holds of the current control line's input files
            String[] hostsOfFile = getSortedHosts(fs, inFiles);

            String outputHosts = "";
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
                outputHosts += host + " ";
            }
            for( String host : theseHosts ) {
                hosts[h] = theseHosts.get(h-hostsOfFile.length);
                h++;
                outputHosts += host + " ";
            }
            
            LOG.debug("sorted hosts: " + outputHosts);

            addToLocationMap(locationMap, hosts, controlLine.toString(), l);
        }
        return locationMap;
    }

    public static void addToLocationMap(
        Map<String, ArrayList<String>> locationMap, String[] hosts,
        String line, int lineNum) {
        LOG.debug("hosts.size = " + hosts.length);
        for (String host : hosts) {
            LOG.debug("  host = " + host);
            ArrayList<String> lines = locationMap.containsKey(host) ? locationMap
                    .get(host) : new ArrayList<String>();
            // if the location is unbalanced (got too few lines) add line for that location
            LOG.debug("  lines.size = " + lines.size());
            if (lines.size() < (float)lineNum / hosts.length ) {
                LOG.debug("  host is unbalanced, adding line to it");
                lines.add(line);
                locationMap.put(host, lines);
                break;
            }
        }
    }

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
        Path[] inFiles;
        int i = 0;
        if (strStdinFile != null) {
            inFiles = new Path[mapInputFileParameters.size() + 1];
            Path p = new Path(strStdinFile);
            if (fs.exists(p)) {
                inFiles[i++] = p;
            }
        } else {
            inFiles = new Path[mapInputFileParameters.size()];
        }

        // TODO traverse directories
        for (String fileRef : mapInputFileParameters.values()) {
            LOG.debug("fileRef = " + fileRef );
            Path p = new Path(fileRef);
            if (fs.exists(p)) {
                inFiles[i++] = p;
            }
        }
        return inFiles;
    }

    public static String[] getSortedHosts(FileSystem fs, Path[] inFiles)
            throws IOException {
        final Map<String, Integer> hostMap = new HashMap<String, Integer>();
        for( Path inFile : inFiles ) {
            LOG.debug("inFile = " + inFile.toString());
            FileStatus s = fs.getFileStatus(inFile);
            BlockLocation[] locations = fs.getFileBlockLocations(s, (long)0, s.getLen());
            for( BlockLocation location : locations ) {
                String[] hosts = location.getHosts();
                LOG.debug("  one blockLocation on: ");
                for( String host : hosts ) {
                    LOG.debug("    host = " + host);
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
            public int compare(String host1, String host2) {
                return hostMap.get(host2) - hostMap.get(host1);
            }
        });
        return hosts.toArray(new String[0]);

    }
}


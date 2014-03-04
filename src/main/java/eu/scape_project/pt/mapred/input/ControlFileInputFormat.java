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
 * NLineInputFormat which splits N lines of input as one split.
 *
 * In many "pleasantly" parallel applications, each process/mapper
 * processes the same input file (s), but with computations are
 * controlled by different parameters.(Referred to as "parameter sweeps").
 * One way to achieve this, is to specify a set of parameters
 * (one set per line) as input in a control file
 * (which is the input path to the map-reduce application,
 * where as the input dataset is specified
 * via a config variable in JobConf.).
 *
 * The NLineInputFormat can be used in such applications, that splits
 * the input file such that by default, one line is fed as
 * a value to one map task, and key is the offset.
 * i.e. (k,v) is (LongWritable, Text).
 * The location hints will span the whole mapred cluster.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ControlFileInputFormat extends FileInputFormat<LongWritable, Text> {
    private static Log LOG = LogFactory.getLog(ControlFileInputFormat.class);
    public static final String LINES_PER_MAP = "mapreduce.input.lineinputformat.linespermap";

    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit genericSplit, TaskAttemptContext context)
            throws IOException {
        context.setStatus(genericSplit.toString());
        return new LineRecordReader();
    }

    /**
     * Logically splits the set of input files for the job, splits N lines
     * of the input as one split.
     *
     * @see FileInputFormat#getSplits(JobContext)
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
            FSDataOutputStream fsout = fs.create(newControlFile);
            int start = 0, byteCounter = 0;

            for (Entry<String, ArrayList<String>> entry : locationMap
                    .entrySet()) {
                String host = entry.getKey();
                LOG.debug(" for host = " + host);
                ArrayList<String> lines = entry.getValue();
                float x = lines.size() / numLinesPerSplit;
                float r = x - (int) x;
                float b = (int) (numLinesPerSplit * r);
                float a = b / (int) x;
                LOG.debug(" x = " + x + ", r = " + r + ", b = " + b + ", a = "
                        + a);
                // x = arraysize / N ... number of splits
                // r = x - int(x) ... fraction of rest
                // b = (int)(N*r) ... number of rest
                // a = b/int(x) ... number of lines per split to add
                // i = 0; j = 0; start = 0

                // if arraysize < N
                if (lines.size() <= numLinesPerSplit) {
                    LOG.debug("arraysize < N ");
                    //   put all lines into newControlFile and use the whole as a split
                    for (String line : lines) {
                        fsout.writeChars(line); // FIXME : add a line break?
                        byteCounter += line.length();
                    }
                    splits.add(new FileSplit(newControlFile, start,
                            byteCounter, new String[] { host }));
                    LOG.debug("split created, start = " + start + ", end = "
                            + byteCounter + ", host = " + host);
                } else {
                    LOG.debug("arraysize > N");
                    int i = 0;
                    float j = 0;
                    for (String line : lines) {
                        LOG.debug("writing line to newControlFile");
                        LOG.debug("line = " + line);
                        fsout.writeChars(line);
                        byteCounter += line.length();
                        if (i < numLinesPerSplit + (int) j) {
                            i++;
                        } else {
                            splits.add(new FileSplit(newControlFile, start,
                                    byteCounter, new String[] { host }));
                            LOG.debug("split created, start = " + start
                                    + ", end = " + byteCounter + ", host = "
                                    + host);
                            if ((int) j == 0) {
                                j += a;
                            } else {
                                j -= (int) j;
                            }
                            i = 0;
                        }
                    }
                }
                start = byteCounter + 1;
            }
            fsout.close();

        } finally {
            if (lr != null) {
                lr.close();
            }
        }
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
        float l = 0;
        while ((lr.readLine(controlLine)) > 0) {
            l += 1;
            LOG.debug("l = " + l);
            // read line by line
            Path[] inFiles = getInputFiles(fs, parser, repo, controlLine.toString());

            // count for each host how many blocks it holds of the current control line's input files
            String[] hosts = getSortedHosts(fs, inFiles);

            LOG.debug("hosts.size = " + hosts.length);
            for (String host : hosts) {
                ArrayList<String> lines = locationMap.containsKey(host) ? locationMap
                        .get(host) : new ArrayList<String>();
                // if the location is unbalanced (got too few lines) add line for that location
                LOG.debug("lines.size = " + lines.size());
                if (lines.size() * hosts.length / l <= 1) {
                    LOG.debug("host is unbalanced, adding line to it");
                    lines.add(controlLine.toString());
                    locationMap.put(host, lines);
                }
            }
        }
        return locationMap;
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

    /**
     * Set the number of lines per split
     * @param job the job to modify
     * @param numLines the number of lines per split
     */
    public static void setNumLinesPerSplit(Job job, int numLines) {
        job.getConfiguration().setInt(LINES_PER_MAP, numLines);
    }

    /**
     * Get the number of lines per split
     * @param job the job
     * @return the number of lines per split
     */
    public static int getNumLinesPerSplit(JobContext job) {
        return job.getConfiguration().getInt(LINES_PER_MAP, 1);
    }
}


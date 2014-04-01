/*
 * Copyright 2013 ait.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.scape_project.pt.mapred.input;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;

import eu.scape_project.pt.repo.LocalToolRepository;
import eu.scape_project.pt.util.PipedArgsParser;

public class ControlFileInputFormatTest {
    private static final Log LOG = LogFactory.getLog(ControlFileInputFormatTest.class);
    
    private LocalToolRepository repo;

    @Before
    public void setUp() throws IOException {
        URL res = this.getClass().getClassLoader().getResource("toolspecs");
        // use the file toolspec xml as the input file too (for this test)
        repo = new LocalToolRepository(res.getFile());
    }
    
    @Test
    public void testGetInputFiles() throws Exception {
        MockupFileSystem fs = new MockupFileSystem();

        String inputFile = "inputFile";
        fs.addFile(inputFile, true, null );
        String line = "\""+inputFile+"\" > foo bar-stdin";
        Path inFiles[] = ControlFileInputFormat.getInputFiles(fs, new PipedArgsParser(), repo, line);

        assertEquals(new Path[]{new Path(inputFile)}, inFiles );

        line = "foo bar --input=\""+inputFile+"\"";
        inFiles = ControlFileInputFormat.getInputFiles(fs, new PipedArgsParser(), repo, line);

        assertEquals(new Path[]{new Path(inputFile)}, inFiles );

        line = "\""+inputFile+"\" > foo bar --input=\""+inputFile+"\"";
        inFiles = ControlFileInputFormat.getInputFiles(fs, new PipedArgsParser(), repo, line);

        assertEquals(new Path[]{new Path(inputFile), new Path(inputFile)}, inFiles );

    }

    @Test
    public void testGetSortedHosts() throws Exception {
        MockupFileSystem fs = new MockupFileSystem();
        Path pInputFile1 = new Path("inputFile1");
        Path pInputFile2 = new Path("inputFile2");
        Path pInputFile3 = new Path("inputFile3");
        fs.addFile(pInputFile1.toString(), true, new BlockLocation[]{
            new BlockLocation(null, new String[]{"hostA", "hostB"}, 0, 0),
            new BlockLocation(null, new String[]{"hostB", "hostC"}, 0, 0)
        });
        fs.addFile(pInputFile2.toString(), true, new BlockLocation[]{
            new BlockLocation(null, new String[]{"hostA"}, 0, 0)
        });
        fs.addFile(pInputFile3.toString(), true, new BlockLocation[]{
            new BlockLocation(null, new String[]{"hostA"}, 0, 0)
        });
        String[] hosts = ControlFileInputFormat.getSortedHosts(fs, new Path[]{
            pInputFile1,
            pInputFile2,
            pInputFile3
        });

        int i = 1;
        for( String host : hosts ) {
            LOG.debug(i++ + ". Host = " + host);
        }

        assertEquals(new String[]{
            "hostA",
            "hostB",
            "hostC"
        }, hosts );

    }

    @Test
    public void testAddToLocationMap() throws Exception {
        Map<String, ArrayList<String>> locationMap = new HashMap<String, ArrayList<String>>();
        String[] hosts = {"hostA", "hostB", "hostC"};

        int l = 0;
        ControlFileInputFormat.addToLocationMap(locationMap, hosts, "line1", ++l );
        Map<String, ArrayList<String>> expectedMap = new HashMap<String, ArrayList<String>>(){{
            put("hostA", new ArrayList<String>(){{
                add("line1");
            }});
        }};
        assertEquals(expectedMap, locationMap);

        ControlFileInputFormat.addToLocationMap(locationMap, hosts, "line2", ++l );
        expectedMap.put("hostB", new ArrayList<String>(){{
            add("line2");
        }});
        assertEquals(expectedMap, locationMap);

        ControlFileInputFormat.addToLocationMap(locationMap, hosts, "line3", ++l );
        expectedMap.put("hostC", new ArrayList<String>(){{
            add("line3");
        }});
        assertEquals(expectedMap, locationMap);

        ControlFileInputFormat.addToLocationMap(locationMap, hosts, "line4", ++l );
        expectedMap.get("hostA").add("line4");
        assertEquals(expectedMap, locationMap);

        ControlFileInputFormat.addToLocationMap(locationMap, hosts, "line5", ++l );
        expectedMap.get("hostB").add("line5");
        assertEquals(expectedMap, locationMap);

        ControlFileInputFormat.addToLocationMap(locationMap, hosts, "line6", ++l );
        expectedMap.get("hostC").add("line6");
        assertEquals(expectedMap, locationMap);
    }

    @Test
    public void testWriteNewControlFileAndCreateSplits() throws Exception {
        MockupFileSystem fs = new MockupFileSystem();
        Path newControlFile = new Path("newControlFile");
        fs.addFile("newControlFile", true, null);
        Map<String, ArrayList<String>> locationMap = new HashMap<String, ArrayList<String>>(){{
            put("host1", new ArrayList<String>(){{
                add("line1-1");
                add("line1-2");
                add("line1-3");
            }});
            put("host2", new ArrayList<String>(){{
                add("line2-1");
                add("line2-2");
                add("line2-3");
                add("line2-4");
                add("line2-5");
                add("line2-6");
            }});
            put("host3", new ArrayList<String>(){{
                add("line3-1");
                add("line3-2");
                add("line3-3");
                add("line3-4");
                add("line3-5");
                add("line3-6");
                add("line3-7");
                add("line3-8");
            }});
            put("host4", new ArrayList<String>(){{
                add("line4-1");
                add("line4-2");
                add("line4-3");
                add("line4-4");
                add("line4-5");
                add("line4-6");
                add("line4-7");
                add("line4-8");
                add("line4-9");
                add("line4-10");
            }});
        }};
        List<FileSplit> splits = ControlFileInputFormat.writeNewControlFileAndCreateSplits(
                newControlFile, fs, locationMap, 3);

        FSDataInputStream bis = fs.open(newControlFile);
        int i = 0;
        for( FileSplit split : splits ) {
            LOG.debug(++i + ".split = " + split.toString());
            byte[] content = new byte[(int)split.getLength()];
            bis.read((int)split.getStart(), content, 0, (int)split.getLength());
            String cont = new String(content);
            LOG.debug("  content = " + new String(content));
            if( cont.startsWith("line1-1") ) {
                String expected = "";
                for( String line : locationMap.get("host1") ) {
                    expected += line + "\n";
                }
                assertEquals(expected, cont);
            } else if( cont.startsWith("line2-1") ) {
                String expected = "";
                int j = 0;
                for( String line : locationMap.get("host2") ) {
                    expected += line + "\n";
                    if( ++j == 3 ) break;
                }
                assertEquals(expected, cont);
            } else if( cont.startsWith("line2-4") ) {
                String expected = "";
                int j = 0;
                for( String line : locationMap.get("host2") ) {
                    if( ++j <= 3 ) continue;
                    expected += line + "\n";
                }
                assertEquals(expected, cont);
            } else if( cont.startsWith("line3-1") ) {
                String expected = "";
                int j = 0;
                for( String line : locationMap.get("host3") ) {
                    expected += line + "\n";
                    if( ++j == 4 ) break;
                }
                assertEquals(expected, cont);
            } else if( cont.startsWith("line3-5") ) {
                String expected = "";
                int j = 0;
                for( String line : locationMap.get("host3") ) {
                    if( ++j <= 4 ) continue;
                    expected += line + "\n";
                }
                assertEquals(expected, cont);
            } else if( cont.startsWith("line4-1") ) {
                String expected = "";
                int j = 0;
                for( String line : locationMap.get("host4") ) {
                    expected += line + "\n";
                    if( ++j == 3 ) break;
                }
                assertEquals(expected, cont);
            } else if( cont.startsWith("line4-4") ) {
                String expected = "";
                int j = 0;
                for( String line : locationMap.get("host4") ) {
                    if( ++j <= 3 ) continue;
                    expected += line + "\n";
                    if( ++j > 7  ) break;
                }
                assertEquals(expected, cont);
            } else if( cont.startsWith("line4-7") ) {
                String expected = "";
                int j = 0;
                for( String line : locationMap.get("host4") ) {
                    if( ++j <= 6 ) continue;
                    expected += line + "\n";
                }
                assertEquals(expected, cont);
            } else {
                fail("wrong split");
            }
        }


    }
}

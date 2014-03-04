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

import java.io.IOException;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
            new BlockLocation(null, new String[]{"hostA", "hostC"}, 0, 0)
        });
        String[] hosts = ControlFileInputFormat.getSortedHosts(fs, new Path[]{
            pInputFile1,
            pInputFile2,
            pInputFile3
        });

        assertEquals(new String[]{
            "hostA",
            "hostB",
            "hostC"
        }, hosts );

    }
}

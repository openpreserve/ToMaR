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

import eu.scape_project.pt.repo.LocalToolRepository;
import eu.scape_project.pt.util.PipedArgsParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;

public class ControlFileInputFormatTest extends Configured {
    private static final Log LOG = LogFactory.getLog(ControlFileInputFormatTest.class);
    
    private LocalToolRepository repo;

    @Before
    public void setUp() throws IOException {
        URL res = this.getClass().getClassLoader().getResource("toolspecs");
        // use the file toolspec xml as the input file too (for this test)
        repo = new LocalToolRepository(res.getFile());
    }
    
    public void testGetInputFiles() throws Exception {
        Configuration conf = this.getConf(); 

        String line = "\"hdfs://inputfile1\" > file identify-stdin";
        ControlFileInputFormat.getInputFiles(FileSystem.get(conf), new PipedArgsParser(), repo, line);
    }
}

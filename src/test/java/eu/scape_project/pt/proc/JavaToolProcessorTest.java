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
package eu.scape_project.pt.proc;

import eu.scape_project.pt.repo.LocalToolRepository;
import eu.scape_project.pt.tool.Tool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.junit.Before;
import org.junit.Test;

import eu.scape_project.pt.proc.HelloWorld;

public class JavaToolProcessorTest extends Configured {
    private static final Log LOG = LogFactory
            .getLog(JavaToolProcessorTest.class);
    
    private LocalToolRepository repo;

    @Before
    public void setUp() throws IOException {
        URL res = this.getClass().getClassLoader().getResource("toolspecs");
        // use the file toolspec xml as the input file too (for this test)
        repo = new LocalToolRepository(res.getFile());
    }
    
    @Test
    public void testJavaTool() throws Exception {
        Tool tool = repo.getTool("helloworld");

        LOG.info("TEST java tool helloworld");

        ToolProcessorBuilder builder = new ToolProcessorBuilder();

        builder.setTool(tool);
        builder.setOperation(builder.findOperation("world"));
        ToolProcessor processor = builder.getProcessor();

        Map<String, String> mapInput = new HashMap<String, String>();
        mapInput.put("say", "Hello" );

        processor.setInputFileParameters( mapInput );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        processor.next( new StreamProcessor(baos));
        try {
            processor.execute();
        } catch ( IOException ex ) {
            LOG.error(
                "Exception during execution (maybe unresolved system dependency?): ",
                    ex);
        }
        LOG.info("output: " + new String(baos.toByteArray()) );
    }



}

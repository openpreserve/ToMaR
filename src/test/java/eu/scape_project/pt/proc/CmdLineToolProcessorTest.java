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
import eu.scape_project.pt.tool.Operation;
import eu.scape_project.pt.tool.Tool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.junit.Before;
import org.junit.Test;

public class CmdLineToolProcessorTest extends Configured {
    private static final Log LOG = LogFactory.getLog(CmdLineToolProcessorTest.class);
    
    private LocalToolRepository repo;

    @Before
    public void setUp() throws IOException {
        URL res = this.getClass().getClassLoader().getResource("toolspecs");
        // use the file toolspec xml as the input file too (for this test)
        repo = new LocalToolRepository(res.getFile());
    }
    
    @Test
    public void testExecuteFileIdentify() throws Exception {
        Tool tool = repo.getTool("file");

        String tmpInputFile = this.getClass().getClassLoader().getResource("ps2pdf-input.ps").getFile();

        LOG.debug("tmpInputFile = " + tmpInputFile );

        LOG.info("TEST file-identify");

        ToolProcessorBuilder builder = new ToolProcessorBuilder();

        builder.setTool(tool);
        builder.setOperation(builder.findOperation("identify"));
        ToolProcessor processor = builder.getProcessor();

        Map<String, String> mapInput = new HashMap<String, String>();
        mapInput.put("input", tmpInputFile );

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

    @Test
    public void testExecuteFileIdentifyStdin() throws Exception {

        LOG.info("TEST file-identify-stdin");

        // This test may throw an "Broken pipe" IOException 
        // because file needs not to read the whole file data from 
        // stdin and will terminate while the other thread is reading streams.

        Tool tool = repo.getTool("file");

        String tmpInputFile = this.getClass().getClassLoader().getResource("ps2pdf-input.ps").getFile();

        ToolProcessorBuilder builder = new ToolProcessorBuilder();
        builder.setTool(tool);
        
        builder.setOperation(builder.findOperation("identify-stdin"));
        ToolProcessor processor = builder.getProcessor();

        FileInputStream fin = new FileInputStream( new File( tmpInputFile ));
        StreamProcessor in = new StreamProcessor(fin);
        in.next(processor);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos = new ByteArrayOutputStream();

        processor.next( new StreamProcessor(baos));
        try {
            in.execute();
        } catch ( IOException ex ) {
            LOG.error(
                "Exception during execution (maybe unresolved system dependency?): ",
                    ex);
        }
        LOG.info("output: " + new String(baos.toByteArray()) );

    }

    @Test
    public void testExecutePs2pdfConvert() throws Exception {
        LOG.info("TEST ps2pdf-convert");
        Tool tool = repo.getTool("ps2pdf");

        String tmpInputFile = this.getClass().getClassLoader().getResource("ps2pdf-input.ps").getFile();

        ToolProcessorBuilder builder = new ToolProcessorBuilder();
        builder.setTool(tool);
        builder.setOperation(builder.findOperation("convert"));
        ToolProcessor processor = builder.getProcessor();

        Map<String, String> mapInput = new HashMap<String, String>();

        LOG.debug("tool = " + tool.getName());

        LOG.debug("tmpInputFile = " + tmpInputFile );

        mapInput = new HashMap<String, String>();
        mapInput.put("input", tmpInputFile );

        String tmpOutputFile = File.createTempFile("ps2pdf", ".pdf").getAbsolutePath();
        LOG.debug("tmpOutputFile = " + tmpOutputFile );

        Map<String, String> mapOutput = new HashMap<String, String>();
        mapOutput.put("output", tmpOutputFile );

        processor.setInputFileParameters( mapInput );
        processor.setOutputFileParameters( mapOutput );
        try {
            processor.execute();
        } catch ( IOException ex ) {
            LOG.error(
                "Exception during execution (maybe unresolved system dependency?): ",
                    ex);
        }
    }

    @Test
    public void testExecutePs2pdfConvertStreamed() throws Exception {
        LOG.info("TEST ps2pdf-convert-streamed");
        Tool tool = repo.getTool("ps2pdf");

        String tmpInputFile = this.getClass().getClassLoader()
                .getResource("ps2pdf-input.ps").getFile();
        String tmpOutputFile = File.createTempFile("ps2pdf", ".pdf").getAbsolutePath();

        ToolProcessorBuilder builder = new ToolProcessorBuilder();
        builder.setTool(tool);
        builder.setOperation(builder.findOperation("convert-streamed"));
        ToolProcessor processor = builder.getProcessor();

        LOG.debug("tmpInputFile = " + tmpInputFile );

        LOG.debug("tmpOutputFile = " + tmpOutputFile );

        FileInputStream fin = new FileInputStream( new File( tmpInputFile ));
        StreamProcessor in = new StreamProcessor(fin);
        in = new StreamProcessor( fin );
        in.next( processor );

        FileOutputStream fout = new FileOutputStream( new File( tmpOutputFile ));
        processor.next(new StreamProcessor(fout));

        try {
            in.execute();
        } catch ( IOException ex ) {
            LOG.error(
                "Exception during execution (maybe unresolved system dependency?): ",
                    ex);
        }
    }

    @Test
    public void testExecuteTarMultipleInputFiles() throws Exception {
        LOG.info("TEST zip");
        Tool tool = repo.getTool("tar");

        String tmpInputFile1 = this.getClass().getClassLoader().getResource("ps2pdf-input.ps").getFile();
        String tmpInputFile2 = this.getClass().getClassLoader().getResource("ps2pdf-output.pdf").getFile();
        String tmpInputFile = tmpInputFile1 + " " + tmpInputFile2;

        ToolProcessorBuilder builder = new ToolProcessorBuilder();
        
        builder.setTool(tool);
        builder.setOperation(builder.findOperation("tar"));
        ToolProcessor processor = builder.getProcessor();
        

        Map<String, String> mapInput = new HashMap<String, String>();

        LOG.debug("tool = " + tool.getName());

        LOG.debug("tmpInputFile = " + tmpInputFile );

        mapInput = new HashMap<String, String>();
        mapInput.put("input", tmpInputFile );

        String tmpOutputFile = File.createTempFile("zipped", ".tar").getAbsolutePath();
        LOG.debug("tmpOutputFile = " + tmpOutputFile );

        Map<String, String> mapOutput = new HashMap<String, String>();
        mapOutput.put("output", tmpOutputFile );

        processor.setInputFileParameters( mapInput );
        processor.setOutputFileParameters( mapOutput );
        try {
            processor.execute();
        } catch ( IOException ex ) {
            LOG.error(
                "Exception during execution (maybe unresolved system dependency?): ",
                    ex);
        }
    }


}

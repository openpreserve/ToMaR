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
package eu.scape_project.pt.util;

import java.io.IOException;

public interface CmdLineParser {

    /**
     * Public interface for parsing a command line. The form of the
     * command line should be (--key="value")* (&lt stdinfile)? (&gt stdoutfile)?.
     * Results can be fetched via getters.
     * @param strCmdLine input String
     */
    void parse(String strCmdLine) throws IOException;

    /**
     * Gets recognized commands. 
     */
    Command[] getCommands();

    /**
     * Gets recognized stdin file name.
     */
    String getStdinFile();

    /**
     * Gets recognized stdout file name.
     */
    String getStdoutFile();

}
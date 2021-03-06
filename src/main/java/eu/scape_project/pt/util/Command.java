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

import java.util.HashMap;
import java.util.Map;

public class Command {
    
    private final String tool;
    private final String action;
    private final Map<String, String> pairs;

    public Command(String tool, String action) {
        this(tool, action, new HashMap<String, String>());
    }

    public Command(String tool, String action, Map<String, String> pairs) {
        this.tool = tool;
        this.action = action;
        this.pairs = pairs;
    }

    public void addPair(String key, String value) {
        pairs.put(key, value);
    }

    public String getTool() {
        return tool;
    }

    public String getAction() {
        return action;
    }

    public Map<String, String> getPairs() {
        return pairs;
    }
    
    @Override
    public boolean equals( Object oo ) {
        if (oo == this) {
            return true;
        }
        if (oo == null || oo.getClass() != getClass()) {
            return false;
        }
        Command o = (Command)oo;
        return this.tool.equals(o.tool) && this.action.equals(o.action)
                && this.pairs.equals(o.pairs);
    }
    
    @Override
    public int hashCode() {
        return this.tool.hashCode() ^ this.action.hashCode() ^ this.pairs.hashCode();
    }

}

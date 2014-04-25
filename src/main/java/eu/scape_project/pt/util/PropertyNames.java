package eu.scape_project.pt.util;

/**
 * Keeps property names used in Hadoop Configuration.
 * 
 * @author Matthias Rella, DME-AIT [myrho]
 */
public class PropertyNames {

    public static final String REDUCE_CLASS = "mapreduce.reduce.class";
    public static final String MAP_CLASS = "mapreduce.map.class";
    public static final String OUTPUT_KEY_CLASS = "mapreduce.output.key.class";
    public static final String OUTPUT_VALUE_CLASS = "mapreduce.output.value.class";
    public static final String INPUTFORMAT_CLASS = "mapreduce.inputformat.class";
    public static final String INFILE = "mapred.input.dir";
    public static final String OUTDIR = "mapred.output.dir";
    public static final String REPO_LOCATION = "REPO_LOCATION";

    // nInputFormat
    public static final String LINES_PER_MAP = "mapreduce.input.lineinputformat.linespermap";

}

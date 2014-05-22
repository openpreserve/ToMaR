package eu.scape_project.pt.udf;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;

import eu.scape_project.pt.ToolWrapper;
import eu.scape_project.pt.util.PropertyNames;

public class ControlLineUDF extends EvalFunc<Tuple> {

	UDFContext context = UDFContext.getUDFContext();
	TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		
		if (input == null) {
			return null;
		}

		if (input.size() != 2) {
			throw new IllegalArgumentException(
					"Tuple needs to contain exactly two arguments (toolspecs_path, control_line)");
		}

		String toolspecsPath = (String) input.get(0);
		String ctrlLine = (String) input.get(1);
		
		try {
			
			Configuration conf = context.getJobConf();
			conf.set(PropertyNames.REPO_LOCATION, toolspecsPath);
			
			System.out.println("CtrlLine: "+ctrlLine);
			System.out.println("toolspecsPath: "+toolspecsPath);
            ToolWrapper toolWrapper = new ToolWrapper();
            toolWrapper.setup(conf);
			String stdOut = toolWrapper.wrap(ctrlLine);
			//System.out.println("ToolWrapper.wrap returns: "+stdOut);
			Tuple tuple = tupleFactory.newTuple(stdOut);
			return tuple;
			
		} catch (Exception e) {
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            return tupleFactory.newTuple(writer.toString());
		}

	}		
}

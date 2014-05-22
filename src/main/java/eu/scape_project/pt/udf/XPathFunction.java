package eu.scape_project.pt.udf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import eu.scape_project.pt.util.SimpleXPath;

/**
 * 
 * @author Umar Maqsud
 * 
 */
public class XPathFunction extends EvalFunc<DataBag> {

    TupleFactory tupleFactory = TupleFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {

        if (input == null) {
            return null;
        }

        if (input.size() != 2) {
            throw new IllegalArgumentException(
                    "Tuple needs to contain only two arguments");
        }

        String expression = (String) input.get(0);
        Tuple xml_tuple = (Tuple) input.get(1);
        String xml = (String) xml_tuple.get(0);
        
        DataBag dataBag = new DefaultDataBag();
        try {

            String[] result = SimpleXPath.parse(expression, new ByteArrayInputStream(xml.getBytes()));


            for ( String res : result ) {
                Tuple tuple = tupleFactory.newTuple(res);
                dataBag.add(tuple);
            }


        } catch (Exception e) {
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            Tuple tuple = tupleFactory.newTuple(writer.toString());
            dataBag.add(tuple);
        }         
        return dataBag;

    }

}

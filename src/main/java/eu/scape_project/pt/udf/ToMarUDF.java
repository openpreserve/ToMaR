/**
 * 
 */
package eu.scape_project.pt.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 *
 * @author Umar Maqsud
 *
 */
public class ToMarUDF extends EvalFunc<Tuple> {
	
	TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		return tupleFactory.newTuple("example xml.");
	}

}
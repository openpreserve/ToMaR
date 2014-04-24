package eu.scape_project.pt.udf;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

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
		
		try {

			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			InputSource is = new InputSource(new StringReader(xml.toString()));

			Document xmlDocument = builder.parse(is);

			XPath xPath = XPathFactory.newInstance().newXPath();

			NodeList nodeList = (NodeList) xPath.compile(expression).evaluate(
					xmlDocument, XPathConstants.NODESET);

			DataBag dataBag = new DefaultDataBag();

			for (int i = 0; i < nodeList.getLength(); i++) {
				Tuple tuple = tupleFactory.newTuple(nodeList.item(i)
						.getFirstChild().getNodeValue());
				dataBag.add(tuple);
			}

			return dataBag;

		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}

	}

}

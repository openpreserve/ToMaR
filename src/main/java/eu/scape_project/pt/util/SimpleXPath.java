package eu.scape_project.pt.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class SimpleXPath {
    public static void main(String[] args) throws XPathExpressionException,
            SAXException, IOException, ParserConfigurationException {
        if( args.length < 1 ) {
            return;
        }
        String[] result;
        if( args.length == 1 ) {
            result = SimpleXPath.parse( args[0], System.in);
        } else {
            result = SimpleXPath.parse( args[0], args[1]);
        }
        for ( String res : result ) {
            System.out.println(res);
        }
    }
    public static String[] parse(String expression, String xml) 
            throws SAXException, IOException, ParserConfigurationException,
            XPathExpressionException {
        return SimpleXPath.parse( expression, new ByteArrayInputStream(xml.getBytes()));
    }

    public static String[] parse(String expression, InputStream xml)
            throws SAXException, IOException, ParserConfigurationException,
            XPathExpressionException {
            DocumentBuilderFactory factory = DocumentBuilderFactory
                    .newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();

            Document xmlDocument = builder.parse(xml);

            XPath xPath = XPathFactory.newInstance().newXPath();

            NodeList nodeList = (NodeList) xPath.compile(expression).evaluate(
                    xmlDocument, XPathConstants.NODESET);
            String result[] = new String[nodeList.getLength()];
            for (int i = 0; i < nodeList.getLength(); i++) {
                result[i] = nodeList.item(i).getFirstChild().getNodeValue();
            }
            return result;
    }
}

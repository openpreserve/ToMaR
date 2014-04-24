REGISTER /home/rainer/data/projects/tomar_udf/tomar/target/tomar-1.4.2-SNAPSHOT-jar-with-dependencies.jar;

DEFINE ToMarService eu.scape_project.pt.udf.CmdToolUDF();
DEFINE XPathService eu.scape_project.pt.udf.XPathFunction();

%DECLARE toolspecs_path 'toolspecs'; 
--%DECLARE toolspecs_path '/home/rainer/data/projects/tomar_udf/toolspecs'; 
%DECLARE xpath_exp1 '/fits/filestatus/valid';

/* STEP 1 in Workflow */

image_pathes = LOAD '$image_pathes' USING PigStorage() AS (image_path: chararray);

/* STEP 2 in Workflow */cd 

--fits_validation = STREAM image_pathes THROUGH fits_stream AS (image_path:chararray, xml_text:chararray);
--fits_validation = STREAM image_pathes THROUGH jhove_stream AS (image_path:chararray, xml_text:chararray);

/* STEP 2 with ToMaR */
fits_validation_tomar = FOREACH image_pathes GENERATE image_path as image_path, ToMarService('$toolspecs_path', CONCAT(CONCAT('fits stdxml --input="hdfs:///user/rainer/files/', image_path), '"')) as xml_text;

/* XPATH */
jhove2 = FOREACH fits_validation_tomar GENERATE image_path, XPathService('$xpath_exp1', xml_text) AS node_list;

--flatten_jhove2_list = FOREACH jhove2 GENERATE image_path, FLATTEN(node_list) as node;

--store fits_validation_tomar into 'output_fits';
store jhove2 into 'output_validation';

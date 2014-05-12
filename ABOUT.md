ToMaR
=====
*Let your Preservation Tools Scale*

What Is ToMaR?
--------------
When dealing with large volumes of files, e.g. in the context of file format migration or file characterisation tasks, a standalone server often cannot provide sufficient throughput to process the data in a feasible period of time. ToMaR provides a simple and flexible solution to run preservation tools on a Hadoop MapReduce cluster in a scalable fashion.
ToMaR enables the use of existing command-line tools and Java applications in Hadoop's distributed environment in a similar way to a Desktop computer without needing to rewrite the tools to take advantage of the specialised environment. By utilizing SCAPE tool specification documents, ToMaR allows users to specify complex command-line patterns as simple keywords, which can be executed on a computer cluster or a single machine. ToMaR is a generic MapReduce application which does not require any programming skills.

In short, With ToMaR you can:

* Run existing tools like FITS or Jpylyzer against large amounts of files
* Execute tools in a scalable fashion on a MapReduce cluster
* Enable scalable workflows which chain together a set of different tools like Fits, Apache Tika, Droid, Unix File
* Process payloads that are simply too big to be computed on a single machine

What Can ToMaR Do For Me?
-------------------------

ToMaR offers the following benefits:

* Easy take up of external tools with a clear mapping between the instructions and the physical invocation of the tool
* Use the SCAPE Toolspec, as well as other existing Toolspecs
* Associate simple keywords with complex command-line patterns
* No programming skills are required as only a control file needs to be set up per job 

ToMaR Can Be Used By
--------------------

ToMaR is for those:

* Who need to make their tools fit for distributed execution
* Who need to integrate distributed tool execution into existing workflows
* Without any programming skills who need a convenient way to configure distributed tool execution

Example
-------

Imagine you are working at the department for digital preservation at a famous national library. Your daily work includes data identification, migration and curation. This means that you will have some experience in using the command shell on a Unix system and writing small scripts to perform a certain workflow effectively. When you have to deal with a few hundreds of files at a time, you will invoke a shell script on one file after the other using a simple loop for automation. 

However, at a certain point you may be faced with a much bigger data set than you are used to: one hundred thousand TIFF images are to be migrated to JPEG2000 images in order to save storage space. Processing these files one after the other, where each single migration takes about half a minute, will take up an entire working day.

Most probably, your organisation will make use of Hadoop clusters, for instance to perform data mining on large collections of text files. If you could run your migration tool in parallel on all these machines, this would speed up your migration task tremendously. However, you may wonder how to do this if you have limited Java programming skills and no experience with the MapReduce programming paradigm.

That's where ToMaR, the Tool-to-MapReduce Wrapper comes in!

Credits
-------

* This work was partially supported by the [SCAPE project](http://scape-project.eu). The SCAPE project is co-funded 
by the European Union under FP7 ICT-2009.4.1 (Grant Agreement number 270137)
* This tool is supported by the [Open Planets Foundation](http://www.openplanetsfoundation.org/). 

Publications
------------

**Blog posts**

* [http://www.openplanetsfoundation.org/blogs/2014-03-14-tomar-how-let-your-preservation-tools-scale](http://www.openplanetsfoundation.org/blogs/2014-03-14-tomar-how-let-your-preservation-tools-scale)
* [http://www.openplanetsfoundation.org/blogs/2014-03-07-some-reflections-scalable-arc-warc-migration](http://www.openplanetsfoundation.org/blogs/2014-03-07-some-reflections-scalable-arc-warc-migration)
* [http://www.openplanetsfoundation.org/blogs/2013-12-16-web-archive-fits-characterisation-using-tomar](http://www.openplanetsfoundation.org/blogs/2013-12-16-web-archive-fits-characterisation-using-tomar)

## 2.0.0

  * non compatible major change:
  * update to toolspec 1.1 schema and reuse from toolwrapper-data, drop duplicated (and possibly outdated) toolspec JAXB classes

## 1.6.1

  * minor code changes/cleanups
  * test also run under Windows

## 1.6.0

  * Added ControlFileInputFormat as the default inputformat for control files. This inputformat reorders the input control file to make data-location-aware splits possible. That means that control lines are assigned to nodes where input file references are local.

## 1.5.2

  * Add UDF for Pig scripts.
  * Decouple ToolWrapper from ToolspecMapper so that the core functionality for ToMaR can be called outside of the Mapper.
  * Standard error stream of the local process is redirected to its standard output stream. In case of an non-zero exit status of the process the output of the process is thrown.
  
## 1.5.0

  * Change of the command line interface to resemble the Streaming API of Hadoop. 

## 1.4.2

  * Command lines wrapping also possible for Windows, using "cmd.exe -c {command}"
  * GenericOptionsParser added to main class

## 1.4.1

  * ToolProcessor can be configured with respect to the working directory to run the command line tool
  * Added a condition for Windows. Command lines are not wrapped in "sh -c {command}" on this operation system as sh does not exist there.
  
## 1.4.0

  * ToolProcessor now wraps command line executions in "sh -c {command}". This enables ToolSpec authors to use the full range of possible shell command lines.

## 1.3.0

  * The command line tool is now executed in the working directory of Hadoop. This enables tools to build up relative directory structures.

## 1.2.0

  * Output parameters can now be directories.

## 1.1.0

  * Input and output file parameters can now contain multiple space-separated file references.

## 1.0.0

  * First productive release.

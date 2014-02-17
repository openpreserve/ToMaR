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

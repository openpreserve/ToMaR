package eu.scape_project.pt.proc;

import eu.scape_project.pt.tool.Input;
import eu.scape_project.pt.tool.Operation;
import eu.scape_project.pt.tool.Operations;
import eu.scape_project.pt.tool.Output;
import eu.scape_project.pt.tool.Parameter;
import eu.scape_project.pt.tool.Tool;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Creates processes for a Tool.
 * 
 * @author Matthias Rella [my_rho]
 */
public class ToolProcessor extends Processor {

	private static Log LOG = LogFactory.getLog(ToolProcessor.class);

	/**
	 * Operation of a Tool to use.
	 */
	private Operation operation;

	/**
	 * Tool to use.
	 */
	private Tool tool;

	/**
	 * Parameters referring to input files.
	 */
	private Map<String, String> mapInputFileParameters;

	/**
	 * Parameters referring to output files.
	 */
	private Map<String, String> mapOutputFileParameters;

	/**
	 * Other Parameters.
	 */
	private Map<String, String> mapOtherParameters;

	/**
	 * Underlying sub-process.
	 */
	private Process proc;

	private File workingDir = null;

	/**
	 * Constructs the processor with a tool and an action of a toolspec.
	 */
	public ToolProcessor(Tool tool) {
		this.tool = tool;
		debugToken = 'T';
	}

	/**
	 * Tries to find a operation of the tool.
	 * 
	 * @param strOp
	 *            Operation
	 */
	public Operation findOperation(String strOp) {
		LOG.debug("findOperation(" + strOp + ")");
		System.out.println("findOperation " + strOp);
		Operations operations = tool.getOperations();
		for (Operation op : operations.getOperation()) {
			LOG.debug("op = " + op.getName());
			System.out.println("findOperation op: " + op.getName());
			if (op.getName().equals(strOp)) {
				return op;
			}
		}
		return null;
	}

	/**
	 * Sets the operation to use for execution.
	 */
	public void setOperation(Operation op) {
		this.operation = op;
	}

	/**
	 * Executes the tool, optionally reading from a previous process (stdin).
	 * All input file parameters need to be local to the machine.
	 */
	@Override
	public int execute() throws Exception {
		LOG.debug("execute");
		System.out.println("execute ");
		Map<String, String> allInputs = new HashMap<String, String>();
		allInputs.putAll(getInputFileParameters());
		allInputs.putAll(getOutputFileParameters());
		allInputs.putAll(getOtherParameters());

		for (String key : allInputs.keySet()) {
			LOG.debug("Key: " + key + " = " + allInputs.get(key));
			System.out.println("execute Key: " + key + " = " + allInputs.get(key));
		}

		String strCmd = replaceAll(this.operation.getCommand(), allInputs);
		LOG.debug("strCmd = " + strCmd);
		System.out.println("execute strCmd = " + strCmd);
		String[] cmd;
		if (System.getProperty("os.name").startsWith("Windows")) {
			strCmd = strCmd.replace("/", "\\");
			cmd = new String[] { "cmd.exe", "/C", strCmd };

			File f = new File(System.getProperty("user.dir"));
			File[] fls = f.listFiles();

			for (int i = 0; i < fls.length; i++) {
				System.out.println(fls[i].getName());
			}

			Path currentRelativePath = Paths.get("");
			String s = currentRelativePath.toAbsolutePath().toString();
			System.out.println("Current relative path is: " + s);
			f = new File(s + "\\tmp");
			fls = f.listFiles();
			if (fls != null) {
				for (int i = 0; i < fls.length; i++) {
					System.out.println(fls[i].getName());
				}
			}

		} else {
			cmd = new String[] { "sh", "-c", strCmd };
		}

		for (int i = 0; i < cmd.length; i++) {
			System.out.println("execute: cmd: " + cmd[i]);
		}

		proc = Runtime.getRuntime().exec(cmd, null, this.workingDir);

		this.setStdIn(proc.getOutputStream());
		this.setStdOut(proc.getInputStream());

		new Thread(this).start();

		if (this.next != null)
			return this.next.execute();

		return proc.waitFor();
	}

	/**
	 * Waits for the sub-process to terminate.
	 */
	@Override
	public int waitFor() throws InterruptedException {
		if (proc == null)
			return 0;
		LOG.debug("waitFor");
		int exitCode = proc.waitFor();

		if (exitCode > 0) {
			System.out.println("waitFor exit code: " + exitCode);
		}

		return exitCode;
	}

	@Override
	public void initialize() {
	}

	/**
	 * Fills all types of parameters with given Map of parameters.
	 */
	public void setParameters(Map<String, String> mapParams) {
		for (Entry<String, String> entry : mapParams.entrySet())
			if (getInputFileParameters().containsKey(entry.getKey()))
				getInputFileParameters().put(entry.getKey(), entry.getValue());
			else if (getOutputFileParameters().containsKey(entry.getKey()))
				getOutputFileParameters().put(entry.getKey(), entry.getValue());
			else if (getOtherParameters().containsKey(entry.getKey()))
				getOtherParameters().put(entry.getKey(), entry.getValue());
	}

	/**
	 * Get input file parameters from the toolspec.
	 */
	public Map<String, String> getInputFileParameters() {
		LOG.debug("getInputFileParameters");
		if (this.mapInputFileParameters != null)
			return this.mapInputFileParameters;
		Map<String, String> parameters = new HashMap<String, String>();

		if (operation.getInputs() != null) {
			for (Input input : operation.getInputs().getInput()) {
				LOG.debug("input = " + input.getName());
				System.out.println("getInputFileParameters input name: " + input.getName() + " value: " + input.getDefaultValue());
				parameters.put(input.getName(), input.getDefaultValue());
			}
		}
		return this.mapInputFileParameters = parameters;

	}

	/**
	 * Get output file parameters from the toolspec.
	 */
	public Map<String, String> getOutputFileParameters() {
		LOG.debug("getOutputFileParameters");
		if (this.mapOutputFileParameters != null)
			return this.mapOutputFileParameters;
		Map<String, String> parameters = new HashMap<String, String>();

		if (operation.getOutputs() != null) {
			for (Output output : operation.getOutputs().getOutput()) {
				LOG.debug("output = " + output.getName());
				System.out.println("output = " + output.getName());
				parameters.put(output.getName(), null);
			}
		}
		return this.mapOutputFileParameters = parameters;
	}

	/**
	 * Gets other input parameters from the toolspec.
	 */
	public Map<String, String> getOtherParameters() {
		if (this.mapOtherParameters != null)
			return this.mapOtherParameters;
		Map<String, String> parameters = new HashMap<String, String>();

		if (operation.getInputs() != null) {
			for (Parameter param : operation.getInputs().getParameter()) {
				parameters.put(param.getName(), param.getDefaultValue());
			}
		}
		return this.mapOtherParameters = parameters;

	}

	/**
	 * Sets input file parameters.
	 * 
	 * @param mapInput
	 *            mapTempInputFileParameters
	 */
	public void setInputFileParameters(Map<String, String> mapInput) {
		this.mapInputFileParameters = mapInput;
	}

	/**
	 * Sets output file parameters.
	 */
	public void setOutputFileParameters(Map<String, String> mapOutput) {
		this.mapOutputFileParameters = mapOutput;
	}

	/**
	 * Replaces ${key}s in given command strCmd by values.
	 */
	private String replaceAll(String strCmd, Map<String, String> mapInputs) {
		if (mapInputs.isEmpty())
			return strCmd;
		// create the pattern wrapping the keys with ${} and join them with '|'
		String regexp = "";
		for (String input : mapInputs.keySet()) {
			regexp += parameterToPlaceholder(input);
		}
		regexp = regexp.substring(0, regexp.length() - 1);
		System.out.println("replaceAll strCmd = " + strCmd);
		LOG.debug("replaceAll.regexp = " + regexp);
		System.out.println("replaceAll.regexp = " + regexp);
		StringBuffer sb = new StringBuffer();
		Pattern p = Pattern.compile(regexp);
		Matcher m = p.matcher(strCmd);

		while (m.find()) {
			String param = placeholderToParameter(m.group());
			System.out.println("replaceAll param: = " + param + " value: " + mapInputs.get(param));
			m.appendReplacement(sb, (mapInputs.get(param).replace("\\", "\\\\")));
		}
		m.appendTail(sb);
		System.out.println("replaceAll strCmd = " + sb.toString());
		return sb.toString();

	}

	/**
	 * Maps a parameter name to the placeholder's form. Inverse of
	 * placeholderToParameter.
	 */
	private String parameterToPlaceholder(String strParameter) {
		return "\\$\\{" + strParameter + "\\}|";
	}

	/**
	 * Maps a placeholder to its parameter name. Inverse of
	 * parameterToPlaceholder.
	 */
	private String placeholderToParameter(String strVariable) {
		return strVariable.substring(2, strVariable.length() - 1);
	}

	public void setWorkingDir(String workingDir) throws IOException {
		System.out.println("setWorkingDir working Dir = " + workingDir);
		File dir = new File(workingDir);
		if (!dir.exists())
			dir.mkdirs();
		else if (!dir.isDirectory())
			throw new IOException("Working directory " + dir + " is not a directory");
		this.workingDir = dir;
	}
}

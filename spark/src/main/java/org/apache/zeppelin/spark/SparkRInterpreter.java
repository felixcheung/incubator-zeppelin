/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.zeppelin.spark;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.ddahl.rscala.java.RClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.LazyOpenInterpreter;
import org.apache.zeppelin.interpreter.WrappedInterpreter;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;

/**
 * SparkR interpreter for Zeppelin.
 *
 */
public class SparkRInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(SparkRInterpreter.class);
  boolean rRunning = false;
  boolean sparkRRunning = false;
  RClient rClient = null;

  static {
    Interpreter.register(
        "sparkr",
        "spark",
        SparkRInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
          .add("spark.home",
               SparkInterpreter.getSystemDefault("SPARK_HOME", "spark.home", ""),
               "Spark home path. Must be provided for SparkR")
          .add("zeppelin.sparkr.packages",
               SparkInterpreter.getSystemDefault(null, "zeppelin.sparkr.packages", ""),
               "Spark packages to load with SparkR").build());
  }

  private class SparkRConf {
    public SparkRConf() {
      sparkMaster = "";
      sparkJars = "";
      sparkEnvir = "";
    }
    public String sparkMaster;
    public String sparkJars;
    public String sparkEnvir;
  }

  public SparkRInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    initializeR();
    rRunning = true;
  }

  @Override
  public void close() {
    if (rRunning) {
      uninitializeR();
    }
  }


  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    if (!rRunning) {
      return new InterpreterResult(Code.ERROR, "R process not running");
    }

    if (!sparkRRunning) {
      SparkRConf srcf = extractSparkConf(getSparkConf());
      if (srcf == null) {
        logger.error("Cannot get SparkConf - is Spark interpreter running?");
      } else {
        try {
          initializeSparkR(srcf, getProperty("spark.home"));
          sparkRRunning = true;
        } catch (Exception err) {
          err.printStackTrace();
          return new InterpreterResult(Code.ERROR, "Cannot initialize SparkR: " + err.toString());
        }
      }
    }

    // Capture output from scala.Console.println, which does not use System.out
    ByteArrayOutputStream overrideStdout = new ByteArrayOutputStream();
    PrintStream stdout = scala.Console.out();
    scala.Console.setOut(new PrintStream(overrideStdout));

    rClient.eval(st);
    scala.Console.setOut(stdout);

    return new InterpreterResult(Code.SUCCESS, overrideStdout.toString());
  }

  @Override
  public void cancel(InterpreterContext context) {
    // Not currently supported
  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    // Not currently supported
    return 0;
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    // Not currently supported
    return new LinkedList<String>();
  }

  private SparkInterpreter getSparkInterpreter() {
    InterpreterGroup intpGroup = getInterpreterGroup();
    synchronized (intpGroup) {
      for (Interpreter intp : getInterpreterGroup()){
        if (intp.getClassName().equals(SparkInterpreter.class.getName())) {
          Interpreter p = intp;
          while (p instanceof WrappedInterpreter) {
            if (p instanceof LazyOpenInterpreter) {
              ((LazyOpenInterpreter) p).open();
            }
            p = ((WrappedInterpreter) p).getInnerInterpreter();
          }
          return (SparkInterpreter) p;
        }
      }
    }
    return null;
  }

  private JavaSparkContext getJavaSparkContext() {
    SparkInterpreter intp = getSparkInterpreter();
    if (intp == null) {
      return null;
    } else {
      return new JavaSparkContext(intp.getSparkContext());
    }
  }

  private SparkConf getSparkConf() {
    JavaSparkContext sc = getJavaSparkContext();
    if (sc == null) {
      return null;
    } else {
      return getJavaSparkContext().getConf();
    }
  }

  private void initializeR() {
    // Ensure R and rScala package are installed - it should exit with code 0
    String rVerOutput = execAndCapture("R --version");
    if (!rVerOutput.startsWith("R version 3")) {
      logger.error(rVerOutput);
      throw new InterpreterException("R version 3 is required");
    }

    String loadLibAndCheckVerCmd = "R -e library(rscala) -e packageVersion('rscala') --slave";
    String rScalaVersionOutput = execAndCapture(loadLibAndCheckVerCmd);
    if (!rScalaVersionOutput.endsWith("‘1.0.6’")) {
      logger.error(rScalaVersionOutput);
      throw new InterpreterException("rScala package version 1.0.6 is required");
    }

    rClient = new RClient();
  }

  private void uninitializeR() {
    rClient.gc();
    rClient.exit();
  }

  private void initializeSparkR(SparkRConf rconf, String sparkHome) {
    assert rClient != null;

    if (sparkHome == null || sparkHome.trim().isEmpty()) {
      // try to get it from environment
      sparkHome = System.getenv("SPARK_HOME");
      if (sparkHome == null || sparkHome.trim().isEmpty()) {
        throw new InterpreterException("SPARK_HOME must be set");
      }
    }

    boolean loadSparkR = rClient.evalB0("require(SparkR)");
    if (!loadSparkR) {
      // Try again from SPARK_HOME
      loadSparkR = rClient.evalB0("require(SparkR, lib.loc='" + sparkHome + "/R/lib')");
      if (!loadSparkR) {
        throw new InterpreterException("SparkR package is not installed or cannot be loaded");
      }
    }

    // sparkR.init(master = "local", appName = "SparkR",
    //  sparkHome = Sys.getenv("SPARK_HOME"), sparkEnvir = list(),
    //  sparkExecutorEnv = list(), sparkJars = "", sparkRLibDir = "")
    String sparkPackages = getProperty("zeppelin.sparkr.packages").trim();
    String sparkRInit = String.format("sc <- sparkR.init(master=\"%s\", "
      + "appName=\"%s\", sparkHome=\"%s\", sparkEnvir=%s, "
      + "sparkJars=\"%s\", sparkPackages = \"%s\")",
      rconf.sparkMaster, "zeppelin-SparkR", sparkHome, rconf.sparkEnvir,
      rconf.sparkJars, sparkPackages);
    logger.info(String.format("Initializing SparkR with: %s", sparkRInit));

    // Initialize SparkR
    rClient.eval(sparkRInit);

    // If SparkR initialized, create sqlContext.
    rClient.eval("sqlContext <- sparkRSQL.init(sc)");
  }

  // Get SparkConf from the existing SparkContext into R code
  // eg. list(spark.executor.memory="1g", spark.master="local[*]")
  private SparkRConf extractSparkConf(SparkConf scf) {
    // Exclude these Spark config value since they should be unique per driver
    HashSet excludeConf = new HashSet();
    excludeConf.add("spark.app.id");
    excludeConf.add("spark.app.name");
    excludeConf.add("spark.driver.host");
    excludeConf.add("spark.driver.port");
    excludeConf.add("spark.driver.extraClassPath");
    excludeConf.add("spark.executor.extraClassPath");
    excludeConf.add("spark.externalBlockStore.folderName");
    excludeConf.add("spark.files");
    excludeConf.add("spark.fileserver.uri");
    excludeConf.add("spark.repl.class.uri");
    excludeConf.add("spark.submit.pyArchives");
    excludeConf.add("spark.tachyonStore.folderName");
    excludeConf.add("spark.yarn.dist.files");

    SparkRConf rconf = new SparkRConf();
    boolean first = true;
    StringBuffer sb = new StringBuffer("list(");
    for (scala.Tuple2<String, String> keyValue : scf.getAll()) {
      // Trim should not be necessary for the key, but let's be safe
      String key = keyValue._1().trim();
      String value = keyValue._2().trim();
      if (key == "spark.master") {
        rconf.sparkMaster = value;
      }
      else if (key == "spark.jars") {
        rconf.sparkJars = value;
      }
      else if (key.startsWith("spark.") && !excludeConf.contains(key) && !value.isEmpty()) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(String.format("%s=\"%s\"", key, value));
        first = false;
      }
    }
    sb.append(")");

    rconf.sparkEnvir = sb.toString();
    return rconf;
  }

  private String execAndCapture(String cmd) {
    return execAndCapture(cmd, true);
  }

  // Return stdout from executing a command if the exit code is 0
  private String execAndCapture(String cmd, boolean required) {
    ArrayList<String> output = new ArrayList<String>();
    int exitValue = -1;
    try {
      String line;
      Process p = Runtime.getRuntime().exec(cmd);
      BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
      while ((line = input.readLine()) != null) {
        output.add(line);
      }
      input.close();
      exitValue = p.waitFor();
    }
    catch (Exception err) {
      err.printStackTrace();
      if (required) {
        logger.error(err.toString());
        throw new InterpreterException(err);
      }
      logger.warn(err.toString());
    }
    String result = "";
    if (exitValue == 0) {
      // flatten array and remove empty line and whitespace.
      String separators = System.getProperty("line.separator");
      result = StringUtils.trim(StringUtils.chomp(StringUtils.join(output, separators)));
    }
    if (StringUtils.isBlank(result)) {
      if (required) {
        String errorMsg = String.format("Command %s not completed successfully.", cmd);
        logger.error(errorMsg);
        throw new InterpreterException(errorMsg);
      } else {
        result = "";
      }
    }
    return result;
  }
}

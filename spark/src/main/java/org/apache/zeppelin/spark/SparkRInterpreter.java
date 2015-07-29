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

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.ServerSocket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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

public class SparkRInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(SparkRInterpreter.class);
  boolean rRunning = false;
  boolean sparkRRunning = false;

  static {
    Interpreter.register(
        "sparkr",
        "spark",
        SparkRInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
          .add("spark.home",
               SparkInterpreter.getSystemDefault("SPARK_HOME", "spark.home", ""),
               "Spark home path. Should be provided for SparkR").build());
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
//
  }


  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    if (!rRunning) {
      return new InterpreterResult(Code.ERROR, "R process not running");
    }

    if (!sparkRRunning) {
      SparkRConf srcf = extractSparkConf(getSparkConf());
      if (srcf == null) {
        logger.warn("Cannot get SparkConf - is Spark interpreter running?");
      } else {
        try {
          initializeSparkR(srcf, getProperty("spark.home"));
        } catch (Exception err) {
          return new InterpreterResult(Code.ERROR, "Cannot initialize SparkR");
        }
      }
    }

    // TODO(felixcheung): r.eval
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
    String cmdLoadLibAndCheckVer = "R -e 'library(rscala); packageVersion(\"rscala\")' --slave";
    String rScalaVersion = execAndCapture(cmdLoadLibAndCheckVer);
    if (!rScalaVersion.endsWith("‘1.0.6’")) {
      throw new InterpreterException("rScala package version 1.0.6 is required");
    }
  }

  private void initializeSparkR(SparkRConf rconf, String sparkHome) {
    boolean loadSparkR = r.eval("require(SparkR)").asBool().isTRUE();
    if (!loadSparkR) {
      throw new InterpreterException("SparkR package not installed");
    }

    if (sparkHome == null || sparkHome.trim().isEmpty()) {
      // try to get it from environment
      sparkHome = System.getenv("SPARK_HOME")
    }

    // sparkR.init(master = "local", appName = "SparkR",
    //  sparkHome = Sys.getenv("SPARK_HOME"), sparkEnvir = list(),
    //  sparkExecutorEnv = list(), sparkJars = "", sparkRLibDir = "")
    String sparkRInit = String.format("sc <- sparkR.init(master=\"%s\",
    appName=\"%s\", sparkHome=\"%s\", sparkEnvir=%s, sparkJars=\"%s\")",
    rconf.sparkMaster, "zeppelin-SparkR", sparkHome, rconf.sparkEnvir, rconf.sparkJars);
    r.eval(sparkRInit);
  }

  // Get SparkConf from the existing SparkContext into R code
  // eg. list(spark.executor.memory="1g", spark.master="local[*]")
  private SparkRConf extractSparkConf(SparkConf scf) {
    // Exclude these Spark config value since they should be unique per driver
    Map excludeConf = new HashSet();
    excludeConf.add("spark.app.id");
    excludeConf.add("spark.app.name");
    excludeConf.add("spark.driver.port");
    excludeConf.add("spark.tachyonStore.folderName");
    excludeConf.add("spark.repl.class.uri");
    excludeConf.add("spark.fileserver.uri");

    SparkRConf rconf = new SparkRConf();
    boolean first = true;
    StringBuffer sb = new StringBuffer("list(");
    for (scala.Tuple2<String,String> keyValue : scf.getAll()) {
      // Trim should not be necessary for the key, but let's be safe
      String key = keyValue._1().trim();
      String value = keyValue._2().trim();
      if (key == "spark.master") {
        rconf.sparkMaster = value;
      }
      else if (key == "spark.jars") {
        rconf.sparkJars = value;
      }
      else if (!excludeConf.contains(key) && !value.isEmpty()) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(String.format("%s=\"%s\"", key, value));
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
      p.exitValue();
    }
    catch (Exception err) {
      err.printStackTrace();
      if (required) {
        logger.error(err.toString());
        throw err;
      }
      logger.warn(err.toString());
    }
    String result = "";
    if (exitValue == 0) {
      // flatten array and remove empty line and whitespace.
      result = StringUtils.trim(StringUtils.chomp(StringUtils.join(output, '\n')));
    }
    if (StringUtils.isBlank(result)) {
      if (required) {
        String errorMsg = String.format("Command %s not completed successfully.", cmd);
        logger.error(errorMsg)
        throw new InterpreterException(errorMsg);
      } else {
        result = "";
      }
    }
    return result;
  }
}

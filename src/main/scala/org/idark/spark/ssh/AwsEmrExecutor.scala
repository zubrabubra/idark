package org.idark.spark.ssh

import java.io.File
import java.util

import com.amazonaws.auth.{BasicAWSCredentials}
import com.amazonaws.services.elasticmapreduce.model.{HadoopJarStepConfig, StepConfig, AddJobFlowStepsRequest}
import com.amazonaws.services.elasticmapreduce.util.StepFactory
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduceClient}

object AwsEmrExecutor {

  def run(uberJarFile: File, className: String, accessKey: String, secretKey: String) = {
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val emr = new AmazonElasticMapReduceClient(credentials)

    val stepFactory = new StepFactory()
    val emrClient = new AmazonElasticMapReduceClient(credentials)
    val req = new AddJobFlowStepsRequest()
    req.withJobFlowId("j-1K48XXXXXXHCB")

    val stepConfigs = new util.ArrayList[StepConfig]()

    val sparkStepConf = new HadoopJarStepConfig()
      .withJar("command-runner.jar")
      .withArgs("spark-submit","--executor-memory","1g","--class",className,uberJarFile.getAbsolutePath,"10")

    val sparkStep = new StepConfig()
      .withName("Spark Step")
      .withActionOnFailure("CONTINUE")
      .withHadoopJarStep(sparkStepConf)

    stepConfigs.add(sparkStep)
    req.withSteps(stepConfigs)
    val result = emrClient.addJobFlowSteps(req)
    result
  }

}

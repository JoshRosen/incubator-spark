package org.apache.spark.scheduler

import org.apache.spark.rdd.RDD
import java.util.Properties
import org.apache.spark.partial.{ApproximateActionListener, PartialResult, ApproximateEvaluator}
import org.apache.spark._
import org.apache.spark.util.TimeStampedHashMap
import scala.collection.mutable
import scala.Some


class RDDScheduler(dagScheduler: DAGScheduler) extends Logging {

  // TODO: expiring cache
  private val shuffleIdToMapStage = new TimeStampedHashMap[Int, ShuffleMapStage]

  /**
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int) {
    logInfo("Asked to cancel job " + jobId)
    eventProcessActor ! JobCancelled(jobId)
  }

  def cancelJobGroup(groupId: String) {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessActor ! JobGroupCancelled(groupId)
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
   */
  def cancelAllJobs() {
    eventProcessActor ! AllJobsCancelled
  }

  private def getParentStages(rdd: RDD[_]): Set[Stage] = {
    val parentStages = new mutable.HashSet[Stage]
    def visit(rdd: RDD[_]) {
      for (dep <- rdd.dependencies) {
        dep match {
          case shufDep: ShuffleDependency[_, _] =>
            parentStages += getOrCreateShuffleMapStage(shufDep)
          case narrowDep: NarrowDependency[_] =>
            visit(narrowDep.rdd)
        }
      }
    }
    visit(rdd)
    parentStages.toSet
  }

  private def createResultStage[T, U](rdd: RDD[T], numPartitions: Int, jobId: JobId, callSite: String,
                                      func: (TaskContext, Iterator[T]) => U) = {
    new ResultStage(dagScheduler, rdd, numPartitions, callSite, func, getParentStages(rdd))
  }

  private def getOrCreateShuffleMapStage(shuffleDep: ShuffleDependency[_,_]): ShuffleMapStage = {
    shuffleIdToMapStage.getOrElseUpdate(shuffleDep.shuffleId, {
      new ShuffleMapStage(dagScheduler, shuffleDep, getParentStages(shuffleDep.rdd))
    })
  }

  /**
   * Submit a job to the job scheduler and get a JobWaiter object back. The JobWaiter object
   * can be used to block until the the job finishes executing or can be used to cancel the job.
   */
  def submitJob[T, U](
     rdd: RDD[T],
     func: (TaskContext, Iterator[T]) => U,
     partitions: Seq[Int],
     callSite: String,
     allowLocal: Boolean,
     resultHandler: (Int, U) => Unit,
     properties: Properties = null): JobWaiter[U] =
  {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }
    val jobId = dagScheduler.getNewJobId
    val stage = createResultStage(rdd, partitions.size, jobId, callSite, func)
    // Maybe run the AllowLocal() stuff here?
    val waiter = new JobWaiter(dagScheduler, jobId, partitions.size, resultHandler)
    dagScheduler.submitJob(stage, jobId, allowLocal, waiter, partitions.toArray, properties)
    waiter
  }

  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: String,
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit,
      properties: Properties = null)
  {
    val waiter = submitJob(rdd, func, partitions, callSite, allowLocal, resultHandler, properties)
    waiter.awaitResult() match {
      case JobSucceeded => {}
      case JobFailed(exception: Exception, _) =>
        logInfo("Failed to run " + callSite)
        throw exception
    }
  }

  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: String,
      timeout: Long,
      properties: Properties = null)
  : PartialResult[R] =
  {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.size).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessActor ! JobSubmitted(jobId, rdd, func2, partitions, allowLocal = false, callSite, listener, properties)
    listener.awaitResult()    // Will throw an exception if the job fails
  }


  /**
   * Run a job on an RDD locally, assuming it has only a single partition and no dependencies.
   * We run the operation in a separate thread just in case it takes a bunch of time, so that we
   * don't block the DAGScheduler event loop or other concurrent jobs.
   */
  protected def runLocally(job: ActiveJob) {
    logInfo("Computing the requested partition locally")
    new Thread("Local computation of job " + job.jobId) {
      override def run() {
        runLocallyWithinThread(job)
      }
    }.start()
  }

  // Broken out for easier testing in DAGSchedulerSuite.
  protected def runLocallyWithinThread(job: ActiveJob) {
    var jobResult: JobResult = JobSucceeded
    try {
      SparkEnv.set(env)
      val rdd = job.finalStage.rdd
      val split = rdd.partitions(job.partitions(0))
      val taskContext =
        new TaskContext(job.finalStage.id, job.partitions(0), 0, runningLocally = true)
      try {
        val result = job.func(taskContext, rdd.iterator(split, taskContext))
        job.listener.taskSucceeded(0, result)
      } finally {
        taskContext.executeOnCompleteCallbacks()
      }
    } catch {
      case e: Exception =>
        jobResult = JobFailed(e, Some(job.finalStage))
        job.listener.jobFailed(e)
    } finally {
      val s = job.finalStage
      // clean up data structures that were populated for a local job,
      // but that won't get cleaned up via the normal paths through
      // completion events or stage abort
      stageIdToJobIds -= s.id
      stageIdToStage -= s.id
      jobIdToStageIds -= job.jobId
      listenerBus.post(SparkListenerJobEnd(job, jobResult))
    }
  }

}

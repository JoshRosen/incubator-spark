/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import scala.collection.mutable
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
import scala.Some


private[spark] object StageState extends Enumeration {
  type StageStage = Value
  val Default, Running, Waiting, Failed = Value
}

private[spark] class StageId(val underlying: Int) extends AnyVal


/**
 * A stage is a set of independent tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * another stage, or a result stage, in which case its tasks directly compute the action that
 * initiated a job (e.g. count(), save(), etc). For shuffle map stages, we also track the nodes
 * that each output partition is on.
 *
 * Each Stage also has a jobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 */
private[spark] abstract class Stage(
    dagScheduler: DAGScheduler,
    val parentStages: Set[Stage]
  ) extends Logging {

  import StageState._

  val stageId = dagScheduler.getNewStageId
  val stageInfo = new StageInfo(this)
  def numTasks: Int

  private var _activeJob: Option[ActiveJob] = None
  def activeJob: Option[ActiveJob] = _activeJob
  def activeJob_=(newActiveJob: Option[ActiveJob]) {
    if (newActiveJob.isDefined) {
      dagScheduler.activeStages += this
      // TODO: this is where I left off; need to refactor this some more.
      dagScheduler.jobIdToStage -= new JobId(newActiveJob.get.jobId)
    } else {
      dagScheduler.activeStages -= this
      dagScheduler.jobIdToStage -= new JobId(_activeJob.get.jobId)
    }
    _activeJob = newActiveJob
  }

  private var _state: StageState.StageStage = Default
  def state: StageState.StageStage = _state
  def state_=(newState: StageState.StageStage) {
    if (newState != state) {
      state match {
        case Running => dagScheduler.runningStages -= this
        case Waiting => dagScheduler.waitingStages -= this
        case Failed  => dagScheduler.failedStages -= this
      }
      newState match {
        case Running => dagScheduler.runningStages += this
        case Waiting => dagScheduler.waitingStages += this
        case Failed  => dagScheduler.failedStages += this
        case Default  => activeJob = None
      }
      _state = newState
     }
  }

  private var nextAttemptId = 0
  def newAttemptId(): Int = {
    val id = nextAttemptId
    nextAttemptId += 1
    id
  }

  dagScheduler.stageIdToStage(stageId) = this

  /** Return a set of tasks needed to recompute this stage */
  def getMissingTasks: Seq[Task[_]]

  def handleTaskSuccess(task: Task[_], result: Any): Boolean

  def name: String

  def isAvailable: Boolean

  val childStages: mutable.Set[Stage] = new mutable.HashSet[Stage]

  /** Missing tasks from this stage */
  val pendingTasks: mutable.Set[Task[_]] = new mutable.HashSet[Task[_]]


  def getAncestorStages(maxDepth: Int = Int.MaxValue,
                        continueSearch: Stage => Boolean = _ => true): Stream[Stage] = {
    val visitedStages = new OpenHashSet[StageId]()
    def visit(stage: Stage, depth: Int): Stream[Stage] = {
      // TODO: would be nice if OpenHashSet, etc. supported +=
      if (depth <= maxDepth && !visitedStages.contains(stage.stageId)) {
        visitedStages.add(stage.stageId)
        stage.parentStages.toStream.filter(continueSearch).flatMap(visit(_, depth + 1))
      } else {
        Stream.empty
      }
    }
    visit(this, 0)
  }

  /**
   * Return true if one of stage's missing ancestor stages is `target`.
   */
  def dependsOn(target: Stage): Boolean = {
    (this == target) || getAncestorStages(continueSearch = !_.isAvailable).contains(this)
  }

  def markAsFinished() {
    val serviceTime = stageInfo.submissionTime match {
      case Some(t) => "%.03f".format((System.currentTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    logInfo("%s (%s) finished in %s s".format(this, name, serviceTime))
    stageInfo.completionTime = Some(System.currentTimeMillis())
    dagScheduler.listenerBus.post(StageCompleted(stageInfo))
    state = Default
  }

  def markAsAborted(reason: String) {
    val job = activeJob.get
    val error = new SparkException("Job aborted: " + reason)
    job.listener.jobFailed(error)
    dagScheduler.listenerBus.post(SparkListenerJobEnd(job, JobFailed(error, Some(this))))
    state = Default
  }

  /**
   * Call when the DAGScheduler is ready to forget about this stage.
   */
  def tryToGarbageCollect() {
    if (childStages.isEmpty && state == Default) {
      logDebug(s"Garbage-collecting $this")
      dagScheduler.stageIdToStage -= stageId
      parentStages.foreach { parent=>
        parent.childStages -= this
        parent.tryToGarbageCollect()
      }
    } else {
      logDebug(s"Not garbage-collecting $this because it has dependent stages or isn't finished")
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Stage => stageId == that.stageId
    case _ => false
  }

  override def hashCode(): Int = stageId.underlying
}

class ShuffleMapStage(
     dagScheduler: DAGScheduler,
     shuffleDep: ShuffleDependency[_, _],
     parentStages: Set[Stage])
  extends Stage(dagScheduler, parentStages) {

  def rdd: RDD[_] = shuffleDep.rdd
  def numTasks: Int = shuffleDep.rdd.partitions.size

  val outputLocs = Array.fill[List[MapStatus]](numTasks)(Nil)
  var numAvailableOutputs = 0

  def addOutputLoc(partition: Int, status: MapStatus) {
    val prevList = outputLocs(partition)
    outputLocs(partition) = status :: prevList
    if (prevList == Nil)
      numAvailableOutputs += 1
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId) {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
      numAvailableOutputs -= 1
    }
  }

  def removeOutputsOnExecutor(execId: String) {
    var becameUnavailable = false
    for (partition <- 0 until numTasks) {
      val prevList = outputLocs(partition)
      val newList = prevList.filterNot(_.location.executorId == execId)
      outputLocs(partition) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        numAvailableOutputs -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, numAvailableOutputs, numTasks, isAvailable))
    }
  }

<<<<<<< Updated upstream
  def newAttemptId(): Int = {
    val id = nextAttemptId
    nextAttemptId += 1
    id
=======
  def getMissingTasks: Seq[Task[_]] = {
    outputLocs.zipWithIndex.filter(_._1 == Nil).map(_._2).map { p =>
      val locs = rdd.getPreferredLocs(p)
      new ShuffleMapTask(stageId.underlying, rdd, shuffleDep, p, locs)
    }
  }

  def handleTaskSuccess(task: Task[_], result: Any): Boolean = {
    val smt = task.asInstanceOf[ShuffleMapTask]
    val status = result.asInstanceOf[MapStatus]
    val execId = status.location.executorId
    logDebug(s"ShuffleMapTask finished on $execId")
    if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
      logInfo("Ignoring possibly bogus ShuffleMapTask completion from " + execId)
    } else {
      addOutputLoc(task.partitionId, status)
    }
    if (state == StageState.Running && pendingTasks.isEmpty) {
      markAsFinished()
      val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      // We supply true to increment the epoch number here in case this is a
      // recomputation of the map outputs. In that case, some nodes may have cached
      // locations with holes (from when we detected the error) and will need the
      // epoch incremented to refetch them.
      // TODO: Only increment the epoch number if this is not the first time
      //       we registered these map outputs.
      mapOutputTracker.registerMapOutputs(
        shuffleDep.shuffleId,
        outputLocs.map(list => if (list.isEmpty) null else list.head).toArray,
        changeEpoch = true)
      if (outputLocs.exists(_ == Nil)) {
        // Some tasks had failed; let's resubmit this stage
        // TODO: Lower-level scheduler should also deal with this
        logInfo("Resubmitting " + this + " (" + this.name +
          ") because some of its tasks had failed: " +
          outputLocs.zipWithIndex.filter(_._1 == Nil).map(_._2).mkString(", "))
        dagScheduler.submitStage(this)
      }
      false
    } else {
      true
    }
>>>>>>> Stashed changes
  }
}

class ResultStage[T, U](
     dagScheduler: DAGScheduler,
     rdd: RDD[U],
     val numTasks: Int,
     callSite: String,
     val func: (TaskContext, Iterator[_]) => _,
     parentStages: Set[Stage])
  extends Stage(dagScheduler, parentStages) {

  def getMissingTasks: Seq[Task] = {
    val job = activeJob.get
    (0 until job.numPartitions).filterNot(job.finished.contains(_)).map { id =>
      val partition = job.partitions(id)
      val locs = rdd.getPreferredLocs(partition)
      new ResultTask(stageId.underlying, rdd, func, partition, locs, id)
    }
  }

  def handleTaskSuccess(task: Task, result: Any): Boolean = {
    val outputId = task.asInstanceOf[ResultTask].outputId
    activeJob match {
      case Some(job) =>
        job.listener.taskSucceeded(outputId, result)
        if (!job.finished(outputId)) {
          job.finished(outputId) = true
          job.numFinished += 1
          // If the whole job has finished, remove it
          if (job.numFinished == job.numPartitions) {
            markAsFinished()
            true
          }
        }
        false
    case None =>
      logInfo(s"Ignoring result from $task because its job has finished")
      false
  }
}

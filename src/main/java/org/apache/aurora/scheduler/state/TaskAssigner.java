/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoGroup;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.mesos.Protos.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.state.TaskAssigner.Status.SUCCESS;
import static org.apache.aurora.scheduler.state.TaskAssigner.Status.VETOED;
import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import static org.apache.mesos.Protos.Offer;

/**
 * Responsible for matching a task against an offer and launching it.
 */
public interface TaskAssigner {
  /**
   * Tries to match a task against an offer.  If a match is found, the assigner makes the
   * appropriate changes to the task and requests task launch.
   *
   * @param storeProvider Storage provider.
   * @param resourceRequest The request for resources being scheduled.
   * @param groupKey Task group key.
   * @param taskIds Task IDs to assign.
   * @param slaveReservations Slave reservations.
   * @return Successfully assigned task IDs.
   */
  Set<String> maybeAssign(
      MutableStoreProvider storeProvider,
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      Iterable<String> taskIds,
      Map<String, TaskGroupKey> slaveReservations);

  class TaskAssignerImpl implements TaskAssigner, PubsubEvent.EventSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger(TaskAssignerImpl.class);

    @VisibleForTesting
    static final Optional<String> LAUNCH_FAILED_MSG =
        Optional.of("Unknown exception attempting to schedule task.");
    @VisibleForTesting
    static final String ASSIGNER_LAUNCH_FAILURES = "assigner_launch_failures";
    @VisibleForTesting
    static final String ASSIGNER_EVALUATED_OFFERS = "assigner_evaluated_offers";

    private final AtomicLong launchFailures;
    private final AtomicLong evaluatedOffers;

    private final StateManager stateManager;
    private final SchedulingFilter filter;
    private final MesosTaskFactory taskFactory;
    private final OfferManager offerManager;
    private final TierManager tierManager;

    // Map with all tasks pending to be executed, and grouped by group key
    private final Map<TaskGroupKey, Iterable<String>> pendingTaskIds = new ConcurrentHashMap<>();

    @Inject
    public TaskAssignerImpl(
        StateManager stateManager,
        SchedulingFilter filter,
        MesosTaskFactory taskFactory,
        OfferManager offerManager,
        TierManager tierManager,
        StatsProvider statsProvider) {

      this.stateManager = requireNonNull(stateManager);
      this.filter = requireNonNull(filter);
      this.taskFactory = requireNonNull(taskFactory);
      this.offerManager = requireNonNull(offerManager);
      this.tierManager = requireNonNull(tierManager);
      this.launchFailures = statsProvider.makeCounter(ASSIGNER_LAUNCH_FAILURES);
      this.evaluatedOffers = statsProvider.makeCounter(ASSIGNER_EVALUATED_OFFERS);
    }

    @VisibleForTesting
    IAssignedTask mapAndAssignResources(Offer offer, IAssignedTask task) {
      IAssignedTask assigned = task;
      for (ResourceType type : ResourceManager.getTaskResourceTypes(assigned)) {
        if (type.getMapper().isPresent()) {
          assigned = type.getMapper().get().mapAndAssign(offer, assigned);
        }
      }
      return assigned;
    }

    private TaskInfo assign(
        MutableStoreProvider storeProvider,
        Offer offer,
        String taskId) {

      String host = offer.getHostname();
      IAssignedTask assigned = stateManager.assignTask(
          storeProvider,
          taskId,
          host,
          offer.getSlaveId(),
          task -> mapAndAssignResources(offer, task));
      LOG.info(
          "Offer on agent {} (id {}) is being assigned task for {}.",
          host, offer.getSlaveId().getValue(), taskId);
      return taskFactory.createFrom(assigned, offer);
    }

    @Timed("assigner_maybe_assign")
    @Override
    public Set<String> maybeAssign(
        MutableStoreProvider storeProvider,
        ResourceRequest resourceRequest,
        TaskGroupKey groupKey,
        Iterable<String> taskIds,
        Map<String, TaskGroupKey> slaveReservations) {

      if (Iterables.isEmpty(taskIds)) {
        return ImmutableSet.of();
      }

      if(!pendingTaskIds.containsKey(groupKey) || !Sets.newHashSet(pendingTaskIds.get(groupKey)).containsAll(Sets.newHashSet(taskIds))) {
        pendingTaskIds.put(groupKey, taskIds);
      }

      return pendingTaskIds.keySet().stream()
              .max(Comparator.comparing(tg -> tg.getTask().getPriority()))
              .map(candidateGroupKey -> {
                LOG.info("GOING TO EXECUTE THIS GROUP: {} - taskIds --> {}", candidateGroupKey.toString(), Lists.newArrayList(pendingTaskIds.get(candidateGroupKey)));
                Set<String> assignedTaskIds = maybeAssignCandidate(storeProvider, resourceRequest, candidateGroupKey, pendingTaskIds.get(candidateGroupKey), slaveReservations);
                LOG.info("Assigned task ids --> {}", assignedTaskIds);
                return assignedTaskIds;
              }).orElse(Collections.emptySet());

//      List<TaskGroupKey> sortedGroupKeys = pendingTaskIds.keySet().stream()
//              .sorted(Comparator.comparing(tg -> tg.getTask().getPriority()))
//              .collect(Collectors.toList());
//
//      for(TaskGroupKey candidateGroupKey : sortedGroupKeys) {
//        TaskAssignerResponse assignedResponse = maybeAssignCandidate(storeProvider, resourceRequest, candidateGroupKey, pendingTaskIds.get(candidateGroupKey), slaveReservations);
//        if(assignedResponse.getTaskIds().containsAll(Sets.newHashSet(pendingTaskIds.get(candidateGroupKey)))) {
//          return assignedResponse.getTaskIds();
//        }
//      }
//
//      return Collections.emptySet();


    }

    private Set<String> maybeAssignCandidate(MutableStoreProvider storeProvider, ResourceRequest resourceRequest, TaskGroupKey groupKey, Iterable<String> taskIds, Map<String, TaskGroupKey> slaveReservations) {
      TierInfo tierInfo = tierManager.getTier(groupKey.getTask());
      ImmutableSet.Builder<String> assignmentResult = ImmutableSet.builder();
      Iterator<String> remainingTasks = taskIds.iterator();
      String taskId = remainingTasks.next();
      Status finalStatus = SUCCESS;

      for (HostOffer offer : offerManager.getOffers(groupKey)) {
        evaluatedOffers.incrementAndGet();

        Optional<TaskGroupKey> reservedGroup = Optional.fromNullable(
            slaveReservations.get(offer.getOffer().getSlaveId().getValue()));

        if (reservedGroup.isPresent() && !reservedGroup.get().equals(groupKey)) {
          // This slave is reserved for a different task group -> skip.
          continue;
        }

        Set<Veto> vetoes = filter.filter(
            new UnusedResource(offer.getResourceBag(tierInfo), offer.getAttributes()),
            resourceRequest);

        if (vetoes.isEmpty()) {
          TaskInfo taskInfo = assign(
              storeProvider,
              offer.getOffer(),
              taskId);

          resourceRequest.getJobState().updateAttributeAggregate(offer.getAttributes());

          try {
            offerManager.launchTask(offer.getOffer().getId(), taskInfo);
            assignmentResult.add(taskId);

            if (remainingTasks.hasNext()) {
              taskId = remainingTasks.next();
            } else {
              break;
            }
          } catch (OfferManager.LaunchException e) {
            LOG.warn("Failed to launch task.", e);
            launchFailures.incrementAndGet();

            // The attempt to schedule the task failed, so we need to backpedal on the
            // assignment.
            // It is in the LOST state and a new task will move to PENDING to replace it.
            // Should the state change fail due to storage issues, that's okay.  The task will
            // time out in the ASSIGNED state and be moved to LOST.
            stateManager.changeState(
                storeProvider,
                taskId,
                Optional.of(PENDING),
                LOST,
                LAUNCH_FAILED_MSG);
            break;
          }
        } else {
          if (Veto.identifyGroup(vetoes) == VetoGroup.STATIC) {
            // Never attempt to match this offer/groupKey pair again.
            offerManager.banOffer(offer.getOffer().getId(), groupKey);
          }
          finalStatus = VETOED;
          LOG.debug("Agent {} vetoed task {}: {}", offer.getOffer().getHostname(), taskId, vetoes);
        }
      }
      return assignmentResult.build();
    }

    /**
     * Informs the task groups of a task state change.
     * <p>
     * This is used to observe {@link org.apache.aurora.gen.ScheduleStatus#PENDING} tasks and begin
     * attempting to schedule them.
     *
     * @param stateChange State change notification.
     */
    @Subscribe
    public synchronized void taskChangedState(TaskStateChange stateChange) {
      if (Tasks.isTerminated(stateChange.getNewState())) {
        pendingTaskIds.remove(TaskGroupKey.from(stateChange.getTask().getAssignedTask().getTask()));
        LOG.info("GROUP REMOVED FROM QUEUE ---> {}", TaskGroupKey.from(stateChange.getTask().getAssignedTask().getTask()).toString());
      }
    }
  }

  final class TaskAssignerResponse {
    private final Set<String> taskIds;
    private final Status status;

    public TaskAssignerResponse(Set<String> taskIds, Status status) {
      this.taskIds = taskIds;
      this.status = status;
    }

    public Set<String> getTaskIds() {
      return taskIds;
    }

    public Status getStatus() {
      return status;
    }
  }

  enum Status {
    SUCCESS,
    VETOED
  }
}

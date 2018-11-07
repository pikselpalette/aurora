package org.apache.aurora.scheduler.state.dsr;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import org.apache.aurora.common.inject.TimedInterceptor;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.TaskAssigner;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.scheduler.state.TaskAssigner.Status.SUCCESS;
import static org.apache.aurora.scheduler.state.TaskAssigner.Status.VETOED;

public class DRFTaskAssigner implements TaskAssigner, PubsubEvent.EventSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger(DRFTaskAssigner.class);

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
    public DRFTaskAssigner(
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
    IAssignedTask mapAndAssignResources(Protos.Offer offer, IAssignedTask task) {
        IAssignedTask assigned = task;
        for (ResourceType type : ResourceManager.getTaskResourceTypes(assigned)) {
            if (type.getMapper().isPresent()) {
                assigned = type.getMapper().get().mapAndAssign(offer, assigned);
            }
        }
        return assigned;
    }

    private Protos.TaskInfo assign(
            Storage.MutableStoreProvider storeProvider,
            Protos.Offer offer,
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

    @TimedInterceptor.Timed("assigner_maybe_assign")
    @Override
    public Set<String> maybeAssign(
            Storage.MutableStoreProvider storeProvider,
            SchedulingFilter.ResourceRequest resourceRequest,
            TaskGroupKey groupKey,
            Iterable<String> taskIds,
            Map<String, TaskGroupKey> slaveReservations) {

        if (Iterables.isEmpty(taskIds)) {
            return ImmutableSet.of();
        }

        if(!pendingTaskIds.containsKey(groupKey) || !Sets.newHashSet(pendingTaskIds.get(groupKey)).containsAll(Sets.newHashSet(taskIds))) {
            pendingTaskIds.put(groupKey, taskIds);
        }

        List<DominantResourceType<Number>> resourceTypes = StreamSupport.stream(offerManager.getOffers(groupKey).spliterator(), false)
                .flatMap(offer -> offer.getOffer().getResourcesList().stream())
                .map(rsrc -> new DominantResourceType(rsrc.getName(), rsrc.getScalar().getValue()))
                .collect(Collectors.<DominantResourceType<Number>>toList());


        List<DRFTask> drfTaskList = pendingTaskIds.keySet().stream()
                .map(tg -> DRFUtils.buildDRFTask(tg, tg.getTask().getNumCpus(), tg.getTask().getDiskMb(), tg.getTask().getRamMb()))
                .sorted(Comparator.<DRFTask>nullsLast(Comparator.comparing(drft -> drft.prioritizedDominantResource(resourceTypes))))
                .collect(Collectors.toList());


        for(DRFTask drfTask : drfTaskList) {
            TaskGroupKey candidateGroupKey = drfTask.getGroupKey();
            LOG.info("DSR task assigner GOING TO EXECUTE THIS GROUP: {} - taskIds --> {}", candidateGroupKey.toString(), Lists.newArrayList(pendingTaskIds.get(candidateGroupKey)));
            if(canBeAssigned(drfTask, resourceTypes)) {
                Set<String> assignedTaskIds = maybeAssignCandidate(storeProvider, resourceRequest, candidateGroupKey, pendingTaskIds.get(candidateGroupKey), slaveReservations);
                LOG.info("Assigned task ids --> {}", assignedTaskIds);
                return assignedTaskIds;
            }
            LOG.warn("Task Group {} cannot be assigned due to there are not enough resources", candidateGroupKey.toString());
        }

//        return pendingTaskIds.keySet().stream()
//                .max(Comparator.comparing(tg -> tg.getTask().getPriority()))
//                .map(candidateGroupKey -> {
//                    LOG.info("DSR task assigner GOING TO EXECUTE THIS GROUP: {} - taskIds --> {}", candidateGroupKey.toString(), Lists.newArrayList(pendingTaskIds.get(candidateGroupKey)));
//                    Set<String> assignedTaskIds = maybeAssignCandidate(storeProvider, resourceRequest, candidateGroupKey, pendingTaskIds.get(candidateGroupKey), slaveReservations);
//                    LOG.info("Assigned task ids --> {}", assignedTaskIds);
//                    return assignedTaskIds;
//                }).orElse(Collections.emptySet());
        return Collections.emptySet();
    }

    private boolean canBeAssigned(DRFTask drfTask, List<DominantResourceType<Number>> resourceTypes) {
        return true;
    }

    private Set<String> maybeAssignCandidate(Storage.MutableStoreProvider storeProvider, SchedulingFilter.ResourceRequest resourceRequest, TaskGroupKey groupKey, Iterable<String> taskIds, Map<String, TaskGroupKey> slaveReservations) {
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

            Set<SchedulingFilter.Veto> vetoes = filter.filter(
                    new SchedulingFilter.UnusedResource(offer.getResourceBag(tierInfo), offer.getAttributes()),
                    resourceRequest);

            if (vetoes.isEmpty()) {
                Protos.TaskInfo taskInfo = assign(
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
                if (SchedulingFilter.Veto.identifyGroup(vetoes) == SchedulingFilter.VetoGroup.STATIC) {
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
    public synchronized void taskChangedState(PubsubEvent.TaskStateChange stateChange) {
        if (Tasks.isTerminated(stateChange.getNewState())) {
            pendingTaskIds.remove(TaskGroupKey.from(stateChange.getTask().getAssignedTask().getTask()));
            LOG.info("GROUP REMOVED FROM QUEUE ---> {}", TaskGroupKey.from(stateChange.getTask().getAssignedTask().getTask()).toString());
        }
    }
}

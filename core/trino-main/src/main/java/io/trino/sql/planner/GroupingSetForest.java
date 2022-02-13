/*
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
package io.trino.sql.planner;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.util.MoreLists.listOfSetsCopy;
import static java.util.Arrays.fill;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;

public class GroupingSetForest
{
    private final List<Set<Symbol>> groupingSets;                       // the id of a grouping set is its index in this list
    private final Map<Integer, Set<Integer>> parentIdToChildrenIds;     // parent id -> children id's
    private final List<Integer> idToParentId;                           // id -> parent id or -1 if id has no parent (i.e. a root)
    private final Set<Integer> rootIds;                                 // id's at the roots of the trees in the forest

    public GroupingSetForest(List<Set<Symbol>> groupingSets, Map<Integer, Set<Integer>> parentIdToChildrenIds)
    {
        requireNonNull(groupingSets, "groupingSets is null");
        requireNonNull(parentIdToChildrenIds, "parentIdToChildrenIds is null");
        this.groupingSets = listOfSetsCopy(groupingSets);
        int numOfGroupingSets = groupingSets.size();
        int[] idToParentId = new int[numOfGroupingSets];    // id -> parent id or -1 if id has no parent (i.e. a root)
        fill(idToParentId, -1);
        this.parentIdToChildrenIds = parentIdToChildrenIds.entrySet().stream()
                .peek(entry -> {
                    int parentId = entry.getKey();
                    checkIdIsValid(parentId, numOfGroupingSets);
                    entry.getValue().forEach(childId -> {
                        checkIdIsValid(childId, numOfGroupingSets);
                        int existingParentId = idToParentId[childId];
                        checkArgument(
                                existingParentId == -1,
                                "Grouping set %s has at least two parents: grouping set %s and grouping set %s",
                                childId,
                                existingParentId,
                                parentId);
                        idToParentId[childId] = parentId;
                        Set<Symbol> parentGroupingSet = this.groupingSets.get(parentId);    // this.groupingSets supports random access
                        Set<Symbol> childGroupingSet = this.groupingSets.get(childId);      // this.groupingSets supports random access
                        checkArgument(
                                parentGroupingSet.containsAll(childGroupingSet),
                                "Grouping set %s (%s) is a parent but not a superset of grouping set %s (%s)",
                                parentId,
                                parentGroupingSet,
                                childId,
                                childGroupingSet);
                    });
                })
                .filter(entry -> !entry.getValue().isEmpty())    // keep only entries whose values are non-empty sets
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableSet.copyOf(entry.getValue())));
        this.idToParentId = checkCycleNotExist(stream(idToParentId).boxed().collect(toImmutableList()));
        rootIds = extractRootIdsFrom(this.idToParentId);
    }

    public GroupingSetForest(List<Set<Symbol>> groupingSets, List<Integer> idToParentId)
    {
        requireNonNull(groupingSets, "groupingSets is null");
        requireNonNull(idToParentId, "idToParentId is null");
        groupingSets = listOfSetsCopy(groupingSets);
        this.groupingSets = groupingSets;
        int numOfGroupingSets = groupingSets.size();
        idToParentId = ImmutableList.copyOf(idToParentId);
        checkArgument(
                idToParentId.size() == numOfGroupingSets,
                "idToParentId's size differs from the number of grouping sets (%s)",
                numOfGroupingSets);
        for (int id = 0; id < idToParentId.size(); id++) {
            int parentId = idToParentId.get(id);
            if (parentId != -1) {
                checkIdIsValid(parentId, numOfGroupingSets);
                Set<Symbol> groupingSet = groupingSets.get(id);
                Set<Symbol> parentGroupingSet = groupingSets.get(parentId);
                checkArgument(
                        parentGroupingSet.containsAll(groupingSet),
                        "Grouping set %s (%s) is a parent but not a superset of grouping set %s (%s)",
                        parentId,
                        parentGroupingSet,
                        id,
                        groupingSet);
            }
        }
        this.idToParentId = checkCycleNotExist(idToParentId);
        Map<Integer, Set<Integer>> parentIdToChildrenIds = new HashMap<>();
        for (int id = 0; id < idToParentId.size(); id++) {
            int parentId = idToParentId.get(id);
            if (parentId != -1) {
                parentIdToChildrenIds.computeIfAbsent(parentId, x -> new HashSet<>()).add(id);
            }
        }
        this.parentIdToChildrenIds = parentIdToChildrenIds.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableSet.copyOf(entry.getValue())));
        rootIds = extractRootIdsFrom(idToParentId);
    }

    private void checkIdIsValid(int id, int numOfGroupingSets)
    {
        checkArgument(
                0 <= id && id < numOfGroupingSets,
                "Grouping set id (%s) should be a non-negative integer less than the number of grouping sets (%s)",
                id,
                numOfGroupingSets);
    }

    private List<Integer> checkCycleNotExist(List<Integer> idToParentId)
    {
        Set<Integer> idsAlreadyChecked = new HashSet<>();
        for (int id = 0; id < idToParentId.size(); id++) {
            if (!idsAlreadyChecked.contains(id)) {
                checkCycleNotExistInReversePathStartingAt(id, idToParentId, idsAlreadyChecked);
            }
        }
        return idToParentId;
    }

    private void checkCycleNotExistInReversePathStartingAt(int startId, List<Integer> idToParentId, Set<Integer> idsAlreadyChecked)
    {
        // follow the (reverse) path [startId, startId's parent, startId's grandparent, ...] to see if there is a cycle
        BiMap<Integer, Integer> path = HashBiMap.create();  // the (reverse) path; 0 -> startId, 1 -> startId's parent, 2 -> startId's grandparent ...
        path.put(0, startId);
        while (true) {
            int parentId = idToParentId.get(path.get(path.size() - 1));
            if (parentId == -1 || idsAlreadyChecked.contains(parentId)) {
                idsAlreadyChecked.addAll(path.values());
                return;
            }
            if (path.containsValue(parentId)) {
                // a cycle is detected; collect the id's in the cycle and throw an exception
                int parentIdIndex = path.inverse().get(parentId);
                List<Integer> idsInCycle = new ArrayList<>();
                for (int i = path.size() - 1; i >= parentIdIndex; i--) {
                    idsInCycle.add(path.get(i));
                }
                throw new IllegalArgumentException("A cycle exists: " + idsInCycle);
            }
            path.put(path.size(), parentId);
        }
    }

    private static Set<Integer> extractRootIdsFrom(List<Integer> idToParentId)
    {
        return IntStream.range(0, idToParentId.size())
                .filter(id -> idToParentId.get(id) == -1)   // any id that has no parent is a root
                .boxed()
                .collect(toImmutableSet());
    }

    public List<Set<Symbol>> getGroupingSets()
    {
        return groupingSets;
    }

    public Map<Integer, Set<Integer>> getParentIdToChildrenIds()
    {
        return parentIdToChildrenIds;
    }

    public List<Integer> getIdToParentId()
    {
        return idToParentId;
    }

    public Set<Integer> getRootIds()
    {
        return rootIds;
    }
}

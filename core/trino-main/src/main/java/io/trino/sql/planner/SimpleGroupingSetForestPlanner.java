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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.RandomAccess;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class SimpleGroupingSetForestPlanner
        implements GroupingSetForestPlanner
{
    @Override
    public GroupingSetForest plan(List<Set<Symbol>> inputGroupingSets)
    {
        requireNonNull(inputGroupingSets, "inputGroupingSets is null");
        List<Set<Symbol>> groupingSets;     // equals inputGroupingSets but also supports random access
        if (inputGroupingSets instanceof RandomAccess) {
            groupingSets = inputGroupingSets;
        }
        else {
            groupingSets = new ArrayList<>(inputGroupingSets);
        }
        int numOfGroupingSets = groupingSets.size();
        List<Integer> sortedIds =   // grouping set id's sorted by the sizes of the grouping sets that they identify
                IntStream.range(0, numOfGroupingSets)
                        .boxed()
                        .sorted(comparingInt(id -> groupingSets.get(id).size()))
                        .collect(toCollection(() -> new ArrayList<>(numOfGroupingSets)));
        Map<Integer, Set<Integer>> parentIdToChildrenIds = new HashMap<>();
        for (int i = 0; i < sortedIds.size(); i++) {
            // search for groupingSet's "parental candidates"; a "parental candidate" of a grouping set s is a grouping set t such that
            //  t is on the right hand side of s in sortedIds,
            //  t is a superset of s, and
            //  t is among the smallest grouping sets satisfying the two conditions above
            OptionalInt firstParentalCandidateIndex = searchForFirstParentalCandidateIndex(i, sortedIds, groupingSets);
            if (firstParentalCandidateIndex.isEmpty()) {
                continue;
            }
            Set<Integer> parentalCandidateIds = new HashSet<>();
            parentalCandidateIds.add(sortedIds.get(firstParentalCandidateIndex.getAsInt()));
            addRemainingParentalCandidateIds(i, firstParentalCandidateIndex.getAsInt(), sortedIds, groupingSets, parentalCandidateIds);
            parentIdToChildrenIds
                    .computeIfAbsent(chooseParentFromCandidates(parentalCandidateIds, parentIdToChildrenIds), parentId -> new HashSet<>())
                    .add(i);
        }
        return new GroupingSetForest(groupingSets, parentIdToChildrenIds);
    }

    private OptionalInt searchForFirstParentalCandidateIndex(
            int indexOfGroupingSetSearchingForParent,
            List<Integer> sortedIds,
            List<Set<Symbol>> groupingSets)
    {
        Set<Symbol> groupingSetSearchingForParent = groupingSets.get(sortedIds.get(indexOfGroupingSetSearchingForParent));
        for (int i = indexOfGroupingSetSearchingForParent + 1; i < sortedIds.size(); i++) {
            if (groupingSets.get(sortedIds.get(i)).containsAll(groupingSetSearchingForParent)) {
                return OptionalInt.of(i);
            }
        }
        return OptionalInt.empty();
    }

    private void addRemainingParentalCandidateIds(
            int indexOfGroupingSetSearchingForParent,
            int firstParentalCandidateIndex,
            List<Integer> sortedIds,
            List<Set<Symbol>> groupingSets,
            Set<Integer> parentalCandidateIds)
    {
        Set<Symbol> groupingSetSearchingForParent = groupingSets.get(sortedIds.get(indexOfGroupingSetSearchingForParent));
        int parentalCandidateSize = groupingSets.get(sortedIds.get(firstParentalCandidateIndex)).size();
        for (int i = firstParentalCandidateIndex + 1; i < sortedIds.size(); i++) {
            int id = sortedIds.get(i);
            Set<Symbol> groupingSet = groupingSets.get(id);
            if (groupingSet.size() > parentalCandidateSize) {
                return;
            }
            if (groupingSet.containsAll(groupingSetSearchingForParent)) {
                parentalCandidateIds.add(id);
            }
        }
    }

    private int chooseParentFromCandidates(Set<Integer> parentalCandidateIds, Map<Integer, Set<Integer>> parentIdToChildrenIds)
    {
        // choose the "best" candidate which is defined to be the candidate with the smallest number of children
        int bestCandidateId = -1;
        int numOfChildrenOfBestCandidate = Integer.MAX_VALUE;
        for (int id : parentalCandidateIds) {
            Set<Integer> children = parentIdToChildrenIds.get(id);
            int numOfChildren = (children == null ? 0 : children.size());
            if (numOfChildren < numOfChildrenOfBestCandidate) {
                bestCandidateId = id;
                numOfChildrenOfBestCandidate = numOfChildren;
            }
        }
        return bestCandidateId;
    }
}

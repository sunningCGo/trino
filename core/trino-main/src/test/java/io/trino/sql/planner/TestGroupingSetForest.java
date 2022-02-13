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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGroupingSetForest
{
    private static final Symbol A = new Symbol("a");
    private static final Symbol B = new Symbol("b");
    private static final Symbol C = new Symbol("c");
    private static final Symbol G = new Symbol("g");
    private static final Symbol H = new Symbol("h");
    private static final Symbol Z = new Symbol("z");

    @Test
    public void testSuccessCases()
    {
        List<Set<Symbol>> groupingSets = ImmutableList.of(
                ImmutableSet.of(A, B, C),   // id 0
                ImmutableSet.of(A),         // id 1
                ImmutableSet.of(A, B),      // id 2
                ImmutableSet.of(A, C),      // id 3
                ImmutableSet.of(B),         // id 4
                ImmutableSet.of(B),         // id 5
                ImmutableSet.of(B),         // id 6

                ImmutableSet.of(G, H),      // id 7
                ImmutableSet.of(G),         // id 8
                ImmutableSet.of(),          // id 9

                ImmutableSet.of(Z));        // id 10
        /*
                         0                  7                10
                       / | \                |
                      2  3  5               8
                     / \    |               |
                    1   4   6               9
        */
        Map<Integer, Set<Integer>> inputParentIdToChildrenIds = ImmutableMap.of(
                0, ImmutableSet.of(2, 3, 5),
                2, ImmutableSet.of(1, 4),
                1, ImmutableSet.of(),   // an id with no children can have an empty children id set (or be omitted from the key set)
                5, ImmutableSet.of(6),
                7, ImmutableSet.of(8),
                8, ImmutableSet.of(9));
        Map<Integer, Set<Integer>> normalizedParentIdToChildrenIds = inputParentIdToChildrenIds.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())    // keep only entries whose values are non-empty sets
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableSet.copyOf(entry.getValue())));
        List<Integer> idToParentId = ImmutableList.of(-1, 2, 0, 0, 2, 0, 5, -1, 7, 8, -1);
        Set<Integer> rootIds = ImmutableSet.of(0, 7, 10);

        assertProperties(
                groupingSets,
                normalizedParentIdToChildrenIds,
                idToParentId,
                rootIds,
                new GroupingSetForest(groupingSets, inputParentIdToChildrenIds),
                new GroupingSetForest(groupingSets, idToParentId));
    }

    @Test
    public void testVertexWithMultipleParents()
    {
        List<Set<Symbol>> groupingSets = ImmutableList.of(
                ImmutableSet.of(A, B),  // id 0
                ImmutableSet.of(A),     // id 1
                ImmutableSet.of(B),     // id 2
                ImmutableSet.of());     // id 3
        /*
                         0
                       /  \
                      1    2
                       \  /
                        3
        */
        Map<Integer, Set<Integer>> parentIdToChildrenIds = ImmutableMap.of(
                0, ImmutableSet.of(1, 2),
                1, ImmutableSet.of(3),
                2, ImmutableSet.of(3));
        assertThatThrownBy(() -> new GroupingSetForest(groupingSets, parentIdToChildrenIds))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Grouping set 3 has at least two parents: grouping set 1 and grouping set 2");
    }

    @Test
    public void testCycleDetection()
    {
        List<Set<Symbol>> groupingSets = ImmutableList.of(
                ImmutableSet.of(A),     // id 0
                ImmutableSet.of(A),     // id 1
                ImmutableSet.of(A),     // id 2
                ImmutableSet.of(A),     // id 3
                ImmutableSet.of());     // id 4
        /*
                         0
                       /  \     2 -> 0
                      1    2
                       \  /     3 -> 2
                        3
                        |
                        4
        */

        Map<Integer, Set<Integer>> parentIdToChildrenIds = ImmutableMap.of(
                0, ImmutableSet.of(1),
                1, ImmutableSet.of(3),
                3, ImmutableSet.of(4, 2),
                2, ImmutableSet.of(0));
        List<Integer> idToParentId = ImmutableList.of(2, 0, 3, 1, 3);

        assertConstructorThrows(
                groupingSets,
                parentIdToChildrenIds,
                idToParentId,
                "A cycle exists");
    }

    @Test
    public void testParentNotASupersetOfChild()
    {
        List<Set<Symbol>> groupingSets = ImmutableList.of(
                ImmutableSet.of(A),         // id 0
                ImmutableSet.of(),          // id 1
                ImmutableSet.of(A, B));     // id 2
        /*
                         0
                       /  \
                      1    2
        */
        Map<Integer, Set<Integer>> parentIdToChildrenIds = ImmutableMap.of(
                0, ImmutableSet.of(1, 2));
        List<Integer> idToParentId = ImmutableList.of(-1, 0, 0);

        assertConstructorThrows(
                groupingSets,
                parentIdToChildrenIds,
                idToParentId,
                "Grouping set 0 ([a]) is a parent but not a superset of grouping set 2 ([a, b])");
    }

    @Test
    public void testForestWithMultipleTreesEachWithOneVertex()
    {
        List<Set<Symbol>> groupingSets = ImmutableList.of(
                ImmutableSet.of(A),         // id 0
                ImmutableSet.of(),          // id 1
                ImmutableSet.of(A, B));     // id 2
        /*
                         0   1   2
        */
        Map<Integer, Set<Integer>> parentIdToChildrenIds = ImmutableMap.of();
        List<Integer> idToParentId = ImmutableList.of(-1, -1, -1);
        Set<Integer> rootIds = ImmutableSet.of(0, 1, 2);

        assertProperties(
                groupingSets,
                parentIdToChildrenIds,
                idToParentId,
                rootIds,
                new GroupingSetForest(groupingSets, parentIdToChildrenIds),
                new GroupingSetForest(groupingSets, idToParentId));
    }

    @Test
    public void testForestWithOneVertex()
    {
        List<Set<Symbol>> groupingSets = ImmutableList.of(
                ImmutableSet.of(A, B, C));  // id 0
        /*
                     0
        */
        Map<Integer, Set<Integer>> parentIdToChildrenIds = ImmutableMap.of();
        List<Integer> idToParentId = ImmutableList.of(-1);
        Set<Integer> rootIds = ImmutableSet.of(0);

        assertProperties(
                groupingSets,
                parentIdToChildrenIds,
                idToParentId,
                rootIds,
                new GroupingSetForest(groupingSets, parentIdToChildrenIds),
                new GroupingSetForest(groupingSets, idToParentId));
    }

    @Test
    public void testEmptyForest()
    {
        List<Set<Symbol>> groupingSets = ImmutableList.of();
        Map<Integer, Set<Integer>> parentIdToChildrenIds = ImmutableMap.of();
        List<Integer> idToParentId = ImmutableList.of();
        Set<Integer> rootIds = ImmutableSet.of();

        assertProperties(
                groupingSets,
                parentIdToChildrenIds,
                idToParentId,
                rootIds,
                new GroupingSetForest(groupingSets, parentIdToChildrenIds),
                new GroupingSetForest(groupingSets, idToParentId));
    }

    private static void assertProperties(
            List<Set<Symbol>> expectedGroupingSets,
            Map<Integer, Set<Integer>> expectedParentIdToChildrenIds,
            List<Integer> expectedIdToParentId,
            Set<Integer> expectedRootIds,
            GroupingSetForest... actualForests)
    {
        for (GroupingSetForest forest : actualForests) {
            assertThat(forest.getGroupingSets()).isEqualTo(expectedGroupingSets);
            assertThat(forest.getParentIdToChildrenIds()).isEqualTo(expectedParentIdToChildrenIds);
            assertThat(forest.getIdToParentId()).isEqualTo(expectedIdToParentId);
            assertThat(forest.getRootIds()).isEqualTo(expectedRootIds);
        }
    }

    private static void assertConstructorThrows(
            List<Set<Symbol>> groupingSets,
            Map<Integer, Set<Integer>> parentIdToChildrenIds,
            List<Integer> idToParentId,
            String expectedMessagePrefix)
    {
        // test the constructor that takes a parentIdToChildrenIds as a parameter
        assertThatThrownBy(() -> new GroupingSetForest(groupingSets, parentIdToChildrenIds))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith(expectedMessagePrefix);

        // test the constructor that takes an idToParentId as a parameter
        assertThatThrownBy(() -> new GroupingSetForest(groupingSets, idToParentId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith(expectedMessagePrefix);
    }
}

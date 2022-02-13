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

import java.util.List;
import java.util.Set;

public interface GroupingSetForestPlanner
{
    /**
     * Plans and returns a {@link GroupingSetForest} for {@code groupingSets}. The (zero-based) index in which a grouping set appears in
     * {@code groupingSets} is used as its id. Let {@code f} be the returned {@code GroupingSetForest}. Then, {@code f.getGroupingSets()}
     * always contains {@code groupingSets} as a prefix (so that the id's of the input grouping sets remain unchanged).
     */
    GroupingSetForest plan(List<Set<Symbol>> groupingSets);
}

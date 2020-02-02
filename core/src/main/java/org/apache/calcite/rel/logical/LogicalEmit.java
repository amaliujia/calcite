/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Emit;
import org.apache.calcite.rex.RexNode;

public class LogicalEmit extends Emit {

  /**
   * Creates a <code>SingleRel</code>.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param input Input relational expression
   */
  protected LogicalEmit(RelOptCluster cluster,
      RelTraitSet traits, RelNode input, RexNode emitExpr) {
    super(cluster, traits, input, emitExpr);
  }

  public static LogicalEmit createEmit(RelNode input, RexNode emitExpr) {
    final RelOptCluster cluster = input.getCluster();
    return new LogicalEmit(cluster, cluster.traitSet(), input, emitExpr);
  }

  public LogicalEmit copy(RelTraitSet traitSet, RelNode input, RexNode emitExpression) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalEmit(getCluster(), traitSet, input, emitExpression);
  }
}

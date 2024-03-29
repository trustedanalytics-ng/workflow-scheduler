/**
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.scheduler.oozie.serialization;

import com.jamesmurty.utils.XMLBuilder2;
import org.trustedanalytics.scheduler.oozie.serialization.WorkflowInstance.WorkflowInstanceBuilder;
import java.util.Objects;

public class DecisionNode implements XmlNode {

    private final String name;
    private final String then;
    private final String orElse;
    private final String condition;

    private DecisionNode(WorkflowDecisionNodeBuilder builder) {
        this.name = Objects.requireNonNull(builder.name, "name");
        this.then = Objects.requireNonNull(builder.then, "then");
        this.orElse = Objects.requireNonNull(builder.orElse, "orElse");
        this.condition = Objects.requireNonNull(builder.condition, "condition");
    }

    public static WorkflowDecisionNodeBuilder builder() {
        return new WorkflowDecisionNodeBuilder();
    }

    @Override
    public XMLBuilder2 asXmlBuilder() {
        return XMLBuilder2.create("decision").a("name",name)
                .e("switch")
                .e("case").a("to", then).t(condition).up()
                .e("default").a("to", orElse).up();
    }

    public static class WorkflowDecisionNodeBuilder implements BuilderNode {

        private WorkflowInstanceBuilder parent;
        private String name;
        private String then;
        private String orElse;
        private String condition;

        protected WorkflowDecisionNodeBuilder setParent(WorkflowInstanceBuilder parent) {
            this.parent = parent;
            return this;
        }

        public WorkflowDecisionNodeBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public WorkflowDecisionNodeBuilder then(String then) {
            this.then = then;
            return this;
        }

        public WorkflowDecisionNodeBuilder orElse(String orElse) {
            this.orElse = orElse;
            return this;
        }

        protected WorkflowDecisionNodeBuilder setCondition(String condition) {
            this.condition = condition;
            return this;
        }

        public WorkflowInstanceBuilder and() {
            return parent;
        }

        @Override
        public DecisionNode build() {
            return new DecisionNode(this);
        }
    }
}

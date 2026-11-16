package leader

import "go.opentelemetry.io/otel/attribute"

func attrComponent(name string) attribute.KeyValue { return attribute.String("lease", name) }

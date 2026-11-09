{{/* Only exact namespaced endpoints and ports are admitted. */}}
{{- define "orca-managed-agents.sandboxEgressSpec" -}}
{{- $egress := .Values.sandboxEgress -}}
{{- $rules := list -}}
{{- range $target := concat (list
      (dict "namespace" $egress.gateway.namespace "podSelector" $egress.gateway.podSelector "ports" (list (dict "port" $egress.gateway.port "protocol" "TCP")))
      (dict "namespace" $egress.dns.namespace "podSelector" $egress.dns.podSelector "ports" (list (dict "port" 53 "protocol" "TCP") (dict "port" 53 "protocol" "UDP")))) $egress.internalBypass -}}
{{- $labels := dict "k8s:io.kubernetes.pod.namespace" $target.namespace -}}
{{- range $key, $value := $target.podSelector -}}
{{- $_ := set $labels (printf "k8s:%s" $key) $value -}}
{{- end -}}
{{- $ports := list -}}
{{- range $port := $target.ports -}}
{{- $ports = append $ports (dict "port" (toString $port.port) "protocol" $port.protocol) -}}
{{- end -}}
{{- $rules = append $rules (dict "toEndpoints" (list (dict "matchLabels" $labels)) "toPorts" (list (dict "ports" $ports))) -}}
{{- end -}}
{{- dict "endpointSelector" (dict "matchLabels" (dict "orca.ai/sandbox-egress" "enabled")) "egress" $rules | toJson -}}
{{- end -}}

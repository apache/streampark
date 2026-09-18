{{ .LicenseContent }}

=======================================================================
Apache StreamPark Subcomponents:

The Apache StreamPark project contains subcomponents with separate copyright
notices and license terms. Your use of the source code for the these
subcomponents is subject to the terms and conditions of the following
licenses.
========================================================================

The GitHub icon in streampark-console-webapp/src/assets/icons/github.svg is from
Ant Design Icons (https://github.com/ant-design/ant-design-icons) and is provided
under the MIT License.
Copyright (c) 2018-present Ant UED, https://xtech.antfin.com/
The text of the license is included in licenses/ui-licenses/license-ant-design-icons.txt.

The GitHub mark is a trademark of GitHub, Inc. The MIT License does not grant
permission to use GitHub trademarks.

{{ range .Groups }}
========================================================================
{{ .LicenseID }} licenses
========================================================================
The following components are provided under the {{ .LicenseID }} License. See project link for details.
{{- if eq .LicenseID "Apache-2.0" }}
The text of each license is the standard Apache 2.0 license.
{{- else }}
The text of each license is also included in licenses/LICENSE-[project].txt.
{{ end }}

    {{- range .Deps }}
      {{- $groupArtifact := regexSplit ":" .Name -1 }}
      {{- if eq (len $groupArtifact) 2 }}
        {{- $group := index $groupArtifact 0 }}
        {{- $artifact := index $groupArtifact 1 }}
    https://mvnrepository.com/artifact/{{ $group }}/{{ $artifact }}/{{ .Version }} {{ .LicenseID }}
      {{- else }}
    https://npmjs.com/package/{{ .Name }}/v/{{ .Version }} {{ .Version }} {{ .LicenseID }}
      {{- end }}
    {{- end }}
{{ end }}

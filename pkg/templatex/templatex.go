package templatex

import (
	"bytes"
	"sync"
	"text/template"
)

var (
	templateCache = sync.Map{} // Cache to store parsed templates
)

func Rander(tpl string, data any) (string, error) {
	// Check if the template is already in the cache
	if cachedTemplate, ok := templateCache.Load(tpl); ok {
		return executeTemplate(cachedTemplate.(*template.Template), data)
	}

	// Parse the template if not in cache
	t, err := template.New("tpl").Parse(tpl)
	if err != nil {
		return "", err
	}

	// Store the parsed template in the cache
	templateCache.Store(tpl, t)

	return executeTemplate(t, data)
}

func executeTemplate(t *template.Template, data any) (string, error) {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

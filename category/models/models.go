package models

type CodeSystem struct {
	Code      string `json:"code"`
	Display   string `json:"display"`
	Reference string `json:"reference"`
}

type Primary struct {
	Type       string       `json:"type"`
	Categories []CodeSystem `json:"categories"`
}

type Secondary struct {
	Type       string       `json:"type"`
	Categories []CodeSystem `json:"categories"`
}

type Tartiary struct {
	Type       string       `json:"type"`
	Categories []CodeSystem `json:"categories"`
}

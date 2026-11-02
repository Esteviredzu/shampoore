package names

import (
	_ "embed"
	"math/rand/v2"
	"strings"
	"sync"
)

//go:embed data/first_male.txt
var firstMaleRaw string

//go:embed data/first_female.txt
var firstFemaleRaw string

//go:embed data/last.txt
var lastRaw string

type lastNameForm struct {
	Male   string
	Female string
}

var (
	loadOnce    sync.Once
	firstMale   []string
	firstFemale []string
	lastNames   []lastNameForm
)

func load() {
	firstMale = splitLines(firstMaleRaw)
	firstFemale = splitLines(firstFemaleRaw)
	for _, line := range splitLines(lastRaw) {
		parts := strings.SplitN(line, ",", 2)
		if len(parts) != 2 {
			continue
		}
		m := strings.TrimSpace(parts[0])
		f := strings.TrimSpace(parts[1])
		if m == "" || f == "" {
			continue
		}
		lastNames = append(lastNames, lastNameForm{Male: m, Female: f})
	}
}

func splitLines(raw string) []string {
	var out []string
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		out = append(out, line)
	}
	return out
}

func Random() string {
	loadOnce.Do(load)
	if len(firstMale) == 0 || len(firstFemale) == 0 || len(lastNames) == 0 {
		return "Гость"
	}
	last := lastNames[rand.IntN(len(lastNames))]
	if rand.IntN(2) == 0 {
		return firstMale[rand.IntN(len(firstMale))] + " " + last.Male
	}
	return firstFemale[rand.IntN(len(firstFemale))] + " " + last.Female
}
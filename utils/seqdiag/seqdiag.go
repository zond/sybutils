package seqdiag

import (
	"fmt"
	"io"
)

type Service struct {
	Doc   *Doc
	Label string
}

type Arrow struct {
	From  *Service
	To    *Service
	Label string
	Color string
	Type  string
}

type Doc struct {
	arrows    []*Arrow
	services  []*Service
	notes     map[string]string
	endPoints map[string]bool
	Label     string
	Flows     []*Flow
}

type Flow struct {
	arrows []*Arrow
	start  *Service
	Color  string
}

var colors []string

func init() {
	colors = []string{
		"blue",
		"crimson",
		"gold",
		"green",
		"magenta",
		"orange",
		"pink",
		"purple",
		"red",
	}
}

func (s *Service) Add(t *Service, l string) *Flow {
	f := &Flow{start: s, Color: colors[len(s.Doc.Flows)%len(colors)]}
	s.Doc.Flows = append(s.Doc.Flows, f)
	f.Add(t, l)
	return f
}

func (self *Flow) Add(t *Service, l string) *Flow {
	doc := self.start.Doc
	doc.endPoints[fmt.Sprintf("%v_%v", self.start.Label, len(doc.arrows))] = true
	doc.endPoints[fmt.Sprintf("%v_%v", t.Label, len(doc.arrows))] = true
	arrow := &Arrow{From: self.start, To: t, Label: l, Color: self.Color, Type: "normal"}
	self.start = t
	doc.arrows = append(doc.arrows, arrow)
	self.arrows = append(self.arrows, arrow)
	if len(self.arrows) > 1 {
		self.arrows[len(self.arrows)-2].Type = "none"
	}
	return self
}

func (self *Flow) AddNote(note string) *Flow {
	key := fmt.Sprintf("Info%d", len(self.start.Doc.arrows)-1)
	self.start.Doc.endPoints[key] = true
	self.start.Doc.notes[key] = note
	return self
}

func (self *Doc) NewService(l string) *Service {
	s := &Service{Label: l, Doc: self}
	self.services = append(self.services, s)
	return s
}

func NewDoc(l string) *Doc {
	return &Doc{
		endPoints: map[string]bool{},
		notes:     map[string]string{},
		Label:     l,
	}
}

func (self *Doc) Generate(b io.Writer) {
	fmt.Fprintf(b, `
digraph %s {
	ranksep=.3; size = "7.5,7.5";
	node [fontsize=10, shape=point, color=grey,  label=""];
	edge [arrowhead=none, style=filled, color="#eeeeee"];
`, self.Label)

	// Plot headers.
	for i := 0; i < len(self.services)-1; i++ {
		fmt.Fprintf(b, "\t%s -> %s [style=invis]\n", self.services[i].Label, self.services[i+1].Label)
	}

	// Plot vertical lines
	for _, service := range self.services {

		fmt.Fprintf(b, "\n\n\t%v [color=black, shape=box, label=\"%v\"];\n",
			service.Label, service.Label)
		last := service.Label
		for i := 0; i < len(self.arrows); i++ {
			key := fmt.Sprintf("%v_%v", service.Label, i)
			if self.endPoints[key] {
				fmt.Fprintf(b, "\t%v -> %v;\n", last, key)
				last = key
			}
		}
		fmt.Fprintf(b, "\t%v -> %s_footer;\n", last, service.Label)
	}

	// Rank header
	fmt.Fprint(b, "\n\n\t{ rank = same; ")
	for _, service := range self.services {
		fmt.Fprintf(b, "%s;\t", service.Label)
	}
	fmt.Fprint(b, "}\n")

	// Rank content
	for i := 0; i < len(self.arrows); i++ {
		fmt.Fprint(b, "\t{ rank = same; ")
		for _, service := range self.services {
			key := fmt.Sprintf("%v_%v", service.Label, i)
			if self.endPoints[key] {
				fmt.Fprintf(b, "%v;\t", key)
			}
		}
		if self.endPoints[fmt.Sprintf("Info%d", i)] {
			fmt.Fprintf(b, "Info%d;", i)
		}
		fmt.Fprintf(b, "}\n")
	}

	// Rank footer
	fmt.Fprint(b, "\t{ rank = same; ")
	for _, service := range self.services {
		fmt.Fprintf(b, "%s_footer;\t", service.Label)
	}
	fmt.Fprint(b, "}\n")

	// Print arrows
	fmt.Fprint(b, "\n\tedge [constraint=false, style=filled, fontsize=8, weight=0, arrowtail=none];\n")

	for i, arrow := range self.arrows {
		fmt.Fprintf(b, "\t%s_%d -> %s_%d [arrowhead=\"%s\" color=\"%s\", label=\"%s\"];\n", arrow.From.Label, i, arrow.To.Label, i, arrow.Type, arrow.Color, arrow.Label)
	}

	for k, v := range self.notes {
		fmt.Fprintf(b, "%s [color=black, shape=larrow, width=1.5, label=\"%s\"];\n", k, v)
	}

	fmt.Fprint(b, "}\n")
}

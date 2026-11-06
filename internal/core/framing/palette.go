package framing

import "fmt"

type Palette struct {
	BitsPerCell int
	Y           []byte
	IdleBGY     byte

	lumaToPalette [256]byte
}

func NewPalette(bits int) (*Palette, error) {
	if bits < 1 || bits > 8 {
		return nil, fmt.Errorf("framing: bits per cell must be 1..8, got %d", bits)
	}
	n := 1 << bits
	step := 256 / n
	y := make([]byte, n)
	for i := 0; i < n; i++ {
		y[i] = byte(step/2 + i*step)
	}
	p := &Palette{
		BitsPerCell: bits,
		Y:           y,
		IdleBGY:     y[0],
	}
	for l := 0; l < 256; l++ {
		best := 0
		bestDist := 1 << 30
		for i, pl := range y {
			d := l - int(pl)
			if d < 0 {
				d = -d
			}
			if d < bestDist {
				bestDist = d
				best = i
			}
		}
		p.lumaToPalette[l] = byte(best)
	}
	return p, nil
}

func (p *Palette) HdrCells() int {
	return (HdrLen*8 + p.BitsPerCell - 1) / p.BitsPerCell
}

func (p *Palette) Shades() int { return len(p.Y) }

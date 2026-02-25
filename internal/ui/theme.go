package ui

import (
	"image/color"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/theme"
)

type paletteTheme struct {
	dark bool
}

func newPaletteTheme(dark bool) fyne.Theme {
	return &paletteTheme{dark: dark}
}

func (t *paletteTheme) Color(name fyne.ThemeColorName, variant fyne.ThemeVariant) color.Color {
	if t.dark {
		switch name {
		case theme.ColorNameBackground:
			return color.NRGBA{R: 9, G: 18, B: 36, A: 255}
		case theme.ColorNameButton:
			return color.NRGBA{R: 32, G: 89, B: 220, A: 255}
		case theme.ColorNameDisabledButton:
			return color.NRGBA{R: 52, G: 64, B: 88, A: 255}
		case theme.ColorNameDisabled:
			return color.NRGBA{R: 108, G: 124, B: 158, A: 255}
		case theme.ColorNameError:
			return color.NRGBA{R: 235, G: 87, B: 87, A: 255}
		case theme.ColorNameFocus:
			return color.NRGBA{R: 53, G: 130, B: 255, A: 255}
		case theme.ColorNameForeground:
			return color.NRGBA{R: 230, G: 238, B: 250, A: 255}
		case theme.ColorNameHover:
			return color.NRGBA{R: 22, G: 35, B: 60, A: 255}
		case theme.ColorNameInputBackground:
			return color.NRGBA{R: 19, G: 32, B: 56, A: 255}
		case theme.ColorNameMenuBackground:
			return color.NRGBA{R: 12, G: 24, B: 44, A: 255}
		case theme.ColorNameOverlayBackground:
			return color.NRGBA{R: 8, G: 16, B: 30, A: 235}
		case theme.ColorNamePlaceHolder:
			return color.NRGBA{R: 140, G: 156, B: 188, A: 255}
		case theme.ColorNamePressed:
			return color.NRGBA{R: 30, G: 75, B: 182, A: 255}
		case theme.ColorNamePrimary:
			return color.NRGBA{R: 37, G: 99, B: 235, A: 255}
		case theme.ColorNameScrollBar:
			return color.NRGBA{R: 67, G: 84, B: 116, A: 220}
		case theme.ColorNameSelection:
			return color.NRGBA{R: 26, G: 52, B: 103, A: 255}
		case theme.ColorNameSeparator:
			return color.NRGBA{R: 29, G: 45, B: 74, A: 255}
		case theme.ColorNameShadow:
			return color.NRGBA{R: 0, G: 0, B: 0, A: 180}
		}
		return theme.DefaultTheme().Color(name, variant)
	}

	switch name {
	case theme.ColorNameBackground:
		return color.NRGBA{R: 244, G: 247, B: 252, A: 255}
	case theme.ColorNameButton:
		return color.NRGBA{R: 34, G: 99, B: 235, A: 255}
	case theme.ColorNameDisabledButton:
		return color.NRGBA{R: 196, G: 206, B: 223, A: 255}
	case theme.ColorNameDisabled:
		return color.NRGBA{R: 120, G: 134, B: 157, A: 255}
	case theme.ColorNameFocus:
		return color.NRGBA{R: 52, G: 105, B: 226, A: 255}
	case theme.ColorNameForeground:
		return color.NRGBA{R: 33, G: 44, B: 65, A: 255}
	case theme.ColorNameHover:
		return color.NRGBA{R: 229, G: 236, B: 249, A: 255}
	case theme.ColorNameInputBackground:
		return color.NRGBA{R: 255, G: 255, B: 255, A: 255}
	case theme.ColorNameMenuBackground:
		return color.NRGBA{R: 249, G: 251, B: 255, A: 255}
	case theme.ColorNameOverlayBackground:
		return color.NRGBA{R: 255, G: 255, B: 255, A: 240}
	case theme.ColorNamePlaceHolder:
		return color.NRGBA{R: 124, G: 138, B: 162, A: 255}
	case theme.ColorNamePressed:
		return color.NRGBA{R: 30, G: 80, B: 188, A: 255}
	case theme.ColorNamePrimary:
		return color.NRGBA{R: 34, G: 99, B: 235, A: 255}
	case theme.ColorNameScrollBar:
		return color.NRGBA{R: 163, G: 178, B: 203, A: 220}
	case theme.ColorNameSelection:
		return color.NRGBA{R: 214, G: 228, B: 252, A: 255}
	case theme.ColorNameSeparator:
		return color.NRGBA{R: 217, G: 225, B: 238, A: 255}
	case theme.ColorNameShadow:
		return color.NRGBA{R: 0, G: 0, B: 0, A: 40}
	}

	return theme.DefaultTheme().Color(name, variant)
}

func (t *paletteTheme) Font(style fyne.TextStyle) fyne.Resource {
	return theme.DefaultTheme().Font(style)
}

func (t *paletteTheme) Icon(name fyne.ThemeIconName) fyne.Resource {
	return theme.DefaultTheme().Icon(name)
}

func (t *paletteTheme) Size(name fyne.ThemeSizeName) float32 {
	return theme.DefaultTheme().Size(name)
}

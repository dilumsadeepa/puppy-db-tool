package main

import (
	"embed"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
	"github.com/wailsapp/wails/v2/pkg/options/linux"
)

//go:embed all:frontend/dist
var assets embed.FS

//go:embed build/appicon.png
var linuxIcon []byte

func main() {
	app := NewApp()

	err := wails.Run(&options.App{
		Title:            "Puppy DB Tool",
		Width:            1480,
		Height:           920,
		MinWidth:         1200,
		MinHeight:        760,
		Frameless:        false,
		AssetServer:      &assetserver.Options{Assets: assets},
		BackgroundColour: &options.RGBA{R: 6, G: 14, B: 30, A: 1},
		Linux: &linux.Options{
			Icon:             linuxIcon,
			ProgramName:      "puppy-db-tool",
			WebviewGpuPolicy: linux.WebviewGpuPolicyNever,
		},
		OnStartup:  app.startup,
		OnShutdown: app.shutdown,
		Bind: []interface{}{
			app,
		},
	})
	if err != nil {
		println("Error:", err.Error())
	}
}

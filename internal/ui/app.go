package ui

import (
	"fmt"
	"image/color"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"

	"puppy-db-tool-desktop/internal/db"
	"puppy-db-tool-desktop/internal/model"
	"puppy-db-tool-desktop/internal/storage"
)

type openTableEntry struct {
	Schema string
	Name   string
}

type DesktopApp struct {
	app fyne.App
	win fyne.Window

	repo      *storage.Repository
	dbService *db.Service

	connections []model.Connection
	snippets    []model.Snippet
	history     []model.QueryHistory

	activeConnectionID string
	selectedConnection string
	selectedSchema     string
	selectedTable      string
	selectedDataRow    int
	sortColumn         string
	sortDesc           bool
	page               int
	totalRows          int64
	hasNextPage        bool

	objects       []model.DBObject
	tableInfosAll []model.TableInfo
	tableInfos    []model.TableInfo
	tableData     model.TableData
	queryResult   model.QueryResult
	openTables    []openTableEntry

	globalSearch string
	darkMode     bool
	showHome     bool

	connList     *widget.List
	connStatus   *widget.Label
	activeDB     *widget.Label
	activeSSH    *widget.Label
	schemaLabel  *widget.Label
	schemaSelect *widget.Select
	tableMeta    *widget.Label
	tableLabel   *widget.Label
	summary      *widget.Label
	pageLabel    *widget.Label
	pagePrevBtn  *widget.Button
	pageNextBtn  *widget.Button
	homeSummary  *widget.Label
	homePageInfo *widget.Label
	homePrevBtn  *widget.Button
	homeNextBtn  *widget.Button
	homeConnList *widget.List

	explorerTree        *widget.Tree
	tableList           *widget.List
	tableGrid           *widget.Table
	openTableList       *widget.List
	openTableTabs       *container.AppTabs
	filterEntry         *widget.Entry
	sortEntry           *widget.Entry
	limitEntry          *widget.Entry
	queryEditor         *widget.Entry
	queryMessage        *widget.Label
	queryDB             *widget.Label
	resultsTable        *widget.Table
	snippetList         *widget.List
	historyList         *widget.List
	tabs                *container.AppTabs
	treeChildren        map[string][]string
	treeLabel           map[string]string
	homeVisibleConnIDs  []int
	selectedObjID       string
	syncingSchemaSelect bool
	syncingTableTabs    bool
	homePage            int
	homePageSize        int
}

func NewDesktopApp() (*DesktopApp, error) {
	store, err := storage.NewStore()
	if err != nil {
		return nil, err
	}
	repo, err := storage.NewRepository(store)
	if err != nil {
		return nil, err
	}

	application := app.NewWithID("io.puppydb.desktop")
	appState := &DesktopApp{
		app:             application,
		repo:            repo,
		dbService:       db.NewService(),
		darkMode:        true,
		showHome:        true,
		page:            1,
		homePage:        1,
		homePageSize:    4,
		treeChildren:    map[string][]string{"root": {}},
		treeLabel:       map[string]string{"root": "Objects"},
		selectedDataRow: -1,
	}

	appState.app.Settings().SetTheme(newPaletteTheme(true))
	appState.win = application.NewWindow("Puppy DB Tool")
	appState.win.Resize(fyne.NewSize(1480, 920))
	appState.win.SetMaster()
	appState.win.SetContent(appState.buildUI())
	appState.win.SetOnClosed(func() {
		appState.dbService.Close()
	})

	appState.reloadPersistedData()
	appState.refreshAllLists()
	return appState, nil
}

func (d *DesktopApp) Run() {
	d.win.ShowAndRun()
}

func (d *DesktopApp) buildUI() fyne.CanvasObject {
	top := d.buildTopBar()
	var left fyne.CanvasObject
	var right fyne.CanvasObject
	if d.showHome {
		left = d.buildHomeSidebar()
		right = d.buildHomeMain()
	} else {
		left = d.buildSidebar()
		right = d.buildMainArea()
	}
	content := container.NewBorder(
		top,
		nil,
		d.pad(left, 6, 6, 6, 4),
		nil,
		d.pad(right, 6, 6, 4, 6),
	)
	return container.NewStack(d.backgroundRect(), content)
}

func (d *DesktopApp) buildTopBar() fyne.CanvasObject {
	iconBg := canvas.NewRectangle(d.accentColor())
	iconBg.CornerRadius = 9
	iconBg.SetMinSize(fyne.NewSize(34, 34))
	logoIcon := widget.NewIcon(theme.StorageIcon())
	logo := container.NewStack(iconBg, container.NewCenter(logoIcon))
	logoTitle := widget.NewLabelWithStyle("DB Explorer", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})

	search := widget.NewEntry()
	search.SetPlaceHolder("Search tables, views, queries...")
	if d.showHome {
		search.SetPlaceHolder("Search connections...")
	}
	search.OnChanged = func(value string) {
		query := strings.ToLower(strings.TrimSpace(value))
		if d.showHome {
			d.globalSearch = query
			d.homePage = 1
			d.refreshHomeConnections()
			return
		}
		d.globalSearch = query
		d.applyTableFilter()
	}
	search.SetText(strings.TrimSpace(d.globalSearch))
	searchField := d.panel(container.NewBorder(
		nil,
		nil,
		d.fixed(widget.NewIcon(theme.SearchIcon()), 20, 20),
		nil,
		search,
	))

	addConn := widget.NewButtonWithIcon("New Connection", theme.ContentAddIcon(), func() {
		d.showConnectionDialog(nil)
	})
	addConn.Importance = widget.HighImportance
	settingsBtn := widget.NewButtonWithIcon("", theme.SettingsIcon(), func() {
		d.darkMode = !d.darkMode
		d.app.Settings().SetTheme(newPaletteTheme(d.darkMode))
		d.win.SetContent(d.buildUI())
		d.syncConnectionHeader()
		d.refreshAllLists()
	})
	notifyBtn := widget.NewButtonWithIcon("", theme.HistoryIcon(), func() {})
	avatar := widget.NewButtonWithIcon("", theme.AccountIcon(), func() {})
	settingsBtn.Importance = widget.LowImportance
	notifyBtn.Importance = widget.LowImportance
	avatar.Importance = widget.LowImportance

	homeBtn := widget.NewButtonWithIcon("", theme.HomeIcon(), func() {
		d.showHome = true
		d.globalSearch = ""
		d.win.SetContent(d.buildUI())
		d.refreshAllLists()
	})
	homeBtn.Importance = widget.LowImportance

	left := container.NewHBox(logo, d.hGap(8), logoTitle)
	if !d.showHome {
		left = container.NewHBox(left, d.hGap(10), d.fixed(homeBtn, 32, 32))
	}
	right := container.NewHBox(
		d.fixed(addConn, 174, 34),
		d.hGap(8),
		d.fixed(settingsBtn, 32, 32),
		d.hGap(6),
		d.fixed(notifyBtn, 32, 32),
		d.hGap(6),
		d.fixed(avatar, 32, 32),
	)
	bar := container.NewBorder(nil, nil, left, right, searchField)
	return d.pad(d.panel(bar), 0, 0, 0, 0)
}

func (d *DesktopApp) buildHomeSidebar() fyne.CanvasObject {
	navTitle := widget.NewLabelWithStyle("NAVIGATION", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	favorites := d.homeNavItem("Favorites", theme.InfoIcon(), false)
	recent := d.homeNavItem("Recent", theme.HistoryIcon(), true)
	project := d.homeNavItem("By Project", theme.FolderIcon(), false)

	clusterTitle := widget.NewLabelWithStyle("CLUSTERS", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	aws := d.homeNavItem("AWS Production", theme.ComputerIcon(), false)
	local := d.homeNavItem("Local Dev", theme.StorageIcon(), false)

	content := container.NewVBox(
		navTitle,
		d.vGap(8),
		favorites,
		d.vGap(6),
		recent,
		d.vGap(6),
		project,
		d.vGap(20),
		clusterTitle,
		d.vGap(8),
		aws,
		d.vGap(6),
		local,
		layout.NewSpacer(),
	)
	return d.fixed(d.panel(content), 270, 840)
}

func (d *DesktopApp) homeNavItem(title string, icon fyne.Resource, active bool) fyne.CanvasObject {
	row := container.NewHBox(
		d.fixed(widget.NewIcon(icon), 18, 18),
		d.hGap(8),
		widget.NewLabelWithStyle(title, fyne.TextAlignLeading, fyne.TextStyle{}),
	)
	if !active {
		return d.pad(row, 8, 8, 8, 8)
	}
	bg := canvas.NewRectangle(color.NRGBA{R: 20, G: 47, B: 90, A: 255})
	bg.CornerRadius = 10
	return container.NewStack(bg, d.pad(row, 8, 8, 10, 10))
}

func (d *DesktopApp) buildHomeMain() fyne.CanvasObject {
	breadcrumb := widget.NewLabel("Dashboard  >  All Connections")
	title := widget.NewLabelWithStyle("Database Connections", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	subtitle := widget.NewLabel("Manage and monitor all your active database endpoints")

	filterBtn := widget.NewButtonWithIcon("", theme.SearchReplaceIcon(), func() {})
	refreshBtn := widget.NewButtonWithIcon("", theme.ViewRefreshIcon(), func() {
		d.refreshHomeConnections()
	})
	filterBtn.Importance = widget.LowImportance
	refreshBtn.Importance = widget.LowImportance

	d.homeConnList = widget.NewList(
		func() int { return len(d.homeVisibleConnIDs) },
		func() fyne.CanvasObject {
			name := widget.NewButton("", nil)
			name.Alignment = widget.ButtonAlignLeading
			name.Importance = widget.LowImportance
			meta := widget.NewLabel("")
			nameCell := container.NewVBox(name, meta)

			host := widget.NewLabel("")
			port := widget.NewLabel("")
			mode := widget.NewLabel("")
			status := widget.NewLabel("")
			action := widget.NewButton("Open", nil)
			action.Importance = widget.LowImportance

			return container.NewGridWithColumns(6, nameCell, host, port, mode, status, action)
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			if id < 0 || id >= len(d.homeVisibleConnIDs) {
				return
			}
			connIndex := d.homeVisibleConnIDs[id]
			if connIndex < 0 || connIndex >= len(d.connections) {
				return
			}
			conn := d.connections[connIndex]
			host, port := connectionHostPort(conn)
			row := obj.(*fyne.Container)
			nameCell := row.Objects[0].(*fyne.Container)
			nameBtn := nameCell.Objects[0].(*widget.Button)
			meta := nameCell.Objects[1].(*widget.Label)
			hostLabel := row.Objects[1].(*widget.Label)
			portLabel := row.Objects[2].(*widget.Label)
			modeLabel := row.Objects[3].(*widget.Label)
			statusLabel := row.Objects[4].(*widget.Label)
			actionBtn := row.Objects[5].(*widget.Button)

			nameBtn.SetText(conn.Name)
			nameBtn.OnTapped = func() { d.openConnectionFromHome(connIndex) }

			service := strings.ToUpper(string(conn.Type))
			if conn.Database != "" {
				meta.SetText(strings.ToLower(conn.Database) + " • " + service)
			} else {
				meta.SetText(service)
			}
			hostLabel.SetText(host)
			portLabel.SetText(strconv.Itoa(port))

			if conn.SSH.Enabled {
				modeLabel.SetText("SSH TUNNEL")
			} else {
				modeLabel.SetText("DIRECT")
			}
			if conn.ID == d.activeConnectionID {
				statusLabel.SetText("● Live")
			} else {
				statusLabel.SetText("● Inactive")
			}

			actionBtn.SetText("Open")
			actionBtn.OnTapped = func() { d.openConnectionFromHome(connIndex) }
		},
	)

	header := container.NewGridWithColumns(
		6,
		widget.NewLabelWithStyle("CONNECTION NAME", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabelWithStyle("HOST ADDRESS", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabelWithStyle("PORT", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabelWithStyle("TYPE", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabelWithStyle("STATUS", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabelWithStyle("ACTIONS", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
	)

	tableCard := d.panel(container.NewBorder(
		container.NewVBox(header, widget.NewSeparator()),
		nil,
		nil,
		nil,
		d.homeConnList,
	))

	d.homeSummary = widget.NewLabel("Showing 0 of 0 connections")
	d.homePageInfo = widget.NewLabel("1 / 1")
	d.homePrevBtn = widget.NewButton("Previous", func() {
		if d.homePage > 1 {
			d.homePage--
			d.refreshHomeConnections()
		}
	})
	d.homeNextBtn = widget.NewButton("Next", func() {
		d.homePage++
		d.refreshHomeConnections()
	})
	d.homePrevBtn.Importance = widget.LowImportance
	d.homeNextBtn.Importance = widget.LowImportance

	pager := container.NewHBox(
		d.fixed(d.homePrevBtn, 104, 32),
		d.hGap(8),
		d.homePageInfo,
		d.hGap(8),
		d.fixed(d.homeNextBtn, 88, 32),
	)

	topHeader := container.NewBorder(
		nil,
		nil,
		container.NewVBox(breadcrumb, title, subtitle),
		container.NewHBox(d.fixed(filterBtn, 36, 36), d.hGap(8), d.fixed(refreshBtn, 36, 36)),
		nil,
	)
	footer := container.NewBorder(nil, nil, d.homeSummary, pager, nil)
	content := container.NewBorder(topHeader, footer, nil, nil, tableCard)
	d.refreshHomeConnections()
	return content
}

func (d *DesktopApp) refreshHomeConnections() {
	if d.homeConnList == nil {
		return
	}

	filtered := d.filteredHomeConnectionIndexes()
	total := len(filtered)
	pageSize := d.homePageSize
	if pageSize <= 0 {
		pageSize = 4
	}

	totalPages := 1
	if total > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}
	if d.homePage < 1 {
		d.homePage = 1
	}
	if d.homePage > totalPages {
		d.homePage = totalPages
	}

	start := 0
	end := 0
	if total > 0 {
		start = (d.homePage - 1) * pageSize
		if start < 0 {
			start = 0
		}
		if start > total {
			start = total
		}
		end = start + pageSize
		if end > total {
			end = total
		}
	}
	d.homeVisibleConnIDs = append([]int(nil), filtered[start:end]...)
	d.homeConnList.Refresh()
	for i := range d.homeVisibleConnIDs {
		d.homeConnList.SetItemHeight(i, 74)
	}

	if d.homeSummary != nil {
		d.homeSummary.SetText(fmt.Sprintf("Showing %d of %d connections", len(d.homeVisibleConnIDs), total))
	}
	if d.homePageInfo != nil {
		d.homePageInfo.SetText(fmt.Sprintf("%d / %d", d.homePage, totalPages))
	}
	if d.homePrevBtn != nil {
		if d.homePage > 1 {
			d.homePrevBtn.Enable()
		} else {
			d.homePrevBtn.Disable()
		}
	}
	if d.homeNextBtn != nil {
		if d.homePage < totalPages {
			d.homeNextBtn.Enable()
		} else {
			d.homeNextBtn.Disable()
		}
	}
}

func (d *DesktopApp) filteredHomeConnectionIndexes() []int {
	if strings.TrimSpace(d.globalSearch) == "" {
		indexes := make([]int, len(d.connections))
		for i := range d.connections {
			indexes[i] = i
		}
		return indexes
	}

	query := strings.ToLower(strings.TrimSpace(d.globalSearch))
	filtered := make([]int, 0, len(d.connections))
	for i, conn := range d.connections {
		host, port := connectionHostPort(conn)
		bag := strings.ToLower(strings.Join([]string{
			conn.Name,
			string(conn.Type),
			conn.Database,
			conn.Schema,
			host,
			strconv.Itoa(port),
		}, " "))
		if strings.Contains(bag, query) {
			filtered = append(filtered, i)
		}
	}
	return filtered
}

func (d *DesktopApp) openConnectionFromHome(index int) {
	d.showHome = false
	d.globalSearch = ""
	d.win.SetContent(d.buildUI())
	d.refreshAllLists()
	d.openConnectionAt(index)
}

func connectionHostPort(conn model.Connection) (string, int) {
	host := strings.TrimSpace(conn.Host)
	port := conn.Port

	if conn.UseConnString && (host == "" || port == 0) {
		switch conn.Type {
		case model.DBTypeMySQL:
			re := regexp.MustCompile(`@tcp\(([^)]+)\)`)
			match := re.FindStringSubmatch(conn.ConnString)
			if len(match) == 2 {
				tcpHost, tcpPort, ok := strings.Cut(match[1], ":")
				host = tcpHost
				if ok {
					if parsed, err := strconv.Atoi(tcpPort); err == nil {
						port = parsed
					}
				}
			}
		default:
			if u, err := url.Parse(conn.ConnString); err == nil {
				if u.Hostname() != "" {
					host = u.Hostname()
				}
				if parsed, err := strconv.Atoi(u.Port()); err == nil {
					port = parsed
				}
			}
		}
	}

	if host == "" {
		host = "localhost"
	}
	if port == 0 {
		port = defaultPort(conn.Type)
	}
	return host, port
}

func (d *DesktopApp) buildSidebar() fyne.CanvasObject {
	title := widget.NewLabelWithStyle("Database Connections", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	d.connStatus = widget.NewLabel("No active connection")
	d.activeDB = widget.NewLabelWithStyle("No active database", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	d.activeSSH = widget.NewLabel("SSH Tunnel: inactive")
	activeCard := d.panel(container.NewVBox(
		widget.NewLabelWithStyle("ACTIVE DATABASE", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		d.activeDB,
		d.activeSSH,
	))

	d.connList = widget.NewList(
		func() int { return len(d.connections) },
		func() fyne.CanvasObject {
			icon := widget.NewIcon(theme.StorageIcon())
			name := widget.NewLabelWithStyle("", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
			meta := widget.NewLabel("")
			return container.NewHBox(icon, container.NewVBox(name, meta))
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			if id < 0 || id >= len(d.connections) {
				return
			}
			conn := d.connections[id]
			root := obj.(*fyne.Container).Objects[1].(*fyne.Container)
			name := root.Objects[0].(*widget.Label)
			meta := root.Objects[1].(*widget.Label)

			prefix := ""
			if conn.ID == d.activeConnectionID {
				prefix = "● "
			}
			name.SetText(prefix + conn.Name)

			mode := "Direct"
			if conn.SSH.Enabled {
				mode = "SSH"
			}
			meta.SetText(fmt.Sprintf("%s | %s", strings.ToUpper(string(conn.Type)), mode))
		},
	)
	d.connList.OnSelected = func(id widget.ListItemID) {
		d.openConnectionAt(id)
	}

	addBtn := widget.NewButton("Add", func() { d.showConnectionDialog(nil) })
	editBtn := widget.NewButton("Edit", func() {
		conn, ok := d.currentSelectedConnection()
		if !ok {
			d.alertError(fmt.Errorf("select a connection first"))
			return
		}
		copy := conn
		d.showConnectionDialog(&copy)
	})
	deleteBtn := widget.NewButton("Delete", func() { d.deleteSelectedConnection() })
	disconnectBtn := widget.NewButton("Disconnect", func() {
		if d.activeConnectionID == "" {
			return
		}
		d.dbService.Disconnect(d.activeConnectionID)
		d.activeConnectionID = ""
		d.openTables = nil
		d.selectedSchema = ""
		d.selectedTable = ""
		d.sortColumn = ""
		d.sortDesc = false
		d.page = 1
		d.totalRows = 0
		d.hasNextPage = false
		d.tableData = model.TableData{}
		d.selectedDataRow = -1
		if d.filterEntry != nil {
			d.filterEntry.SetText("")
		}
		if d.sortEntry != nil {
			d.sortEntry.SetText("")
		}
		d.refreshOpenTableViews()
		d.connStatus.SetText("Disconnected")
		d.syncConnectionHeader()
		d.refreshAllLists()
	})
	addBtn.Importance = widget.HighImportance

	buttons := container.NewVBox(
		container.NewHBox(d.fixed(addBtn, 94, 30), d.hGap(6), d.fixed(editBtn, 94, 30)),
		d.vGap(4),
		container.NewHBox(d.fixed(deleteBtn, 94, 30), d.hGap(6), d.fixed(disconnectBtn, 94, 30)),
	)
	connPanel := d.fixed(d.panel(container.NewBorder(
		container.NewVBox(title, d.connStatus),
		buttons,
		nil,
		nil,
		d.fixed(d.connList, 250, 388),
	)), 270, 560)

	sidebar := container.NewVBox(activeCard, d.vGap(8), connPanel)
	d.syncConnectionHeader()
	return sidebar
}

func (d *DesktopApp) buildMainArea() fyne.CanvasObject {
	explorerTab := container.NewTabItemWithIcon("Explorer", theme.FolderOpenIcon(), d.buildExplorerView())
	tableTab := container.NewTabItemWithIcon("Table Data", theme.ViewFullScreenIcon(), d.buildTableDataView())
	queryTab := container.NewTabItemWithIcon("Query Studio", theme.ComputerIcon(), d.buildQueryView())
	d.tabs = container.NewAppTabs(explorerTab, tableTab, queryTab)
	d.tabs.SetTabLocation(container.TabLocationTop)
	return d.panel(d.tabs)
}

func (d *DesktopApp) buildExplorerView() fyne.CanvasObject {
	d.explorerTree = widget.NewTree(
		func(uid string) []string {
			children, ok := d.treeChildren[uid]
			if !ok {
				return nil
			}
			return children
		},
		func(uid string) bool {
			children, ok := d.treeChildren[uid]
			return ok && len(children) > 0
		},
		func(branch bool) fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(uid string, branch bool, obj fyne.CanvasObject) {
			label := obj.(*widget.Label)
			label.SetText(d.treeLabel[uid])
		},
	)
	d.explorerTree.OpenBranch("root")
	d.explorerTree.OnSelected = func(uid string) {
		d.selectedObjID = uid
		if strings.HasPrefix(uid, "schema|") {
			parts := strings.Split(uid, "|")
			if len(parts) >= 2 {
				d.selectedSchema = parts[1]
				d.loadTableOverview(d.selectedSchema)
			}
			return
		}
		if strings.HasPrefix(uid, "obj|") {
			parts := strings.Split(uid, "|")
			if len(parts) < 4 {
				return
			}
			objType := parts[2]
			if objType == "table" || objType == "view" || objType == "collection" || objType == "string" || objType == "list" || objType == "hash" || objType == "set" || objType == "zset" {
				d.openTable(parts[1], parts[3])
			}
		}
	}

	d.schemaLabel = widget.NewLabel("Schema: -")
	d.schemaSelect = widget.NewSelect([]string{}, func(value string) {
		if d.syncingSchemaSelect {
			return
		}
		if strings.TrimSpace(value) == "" || value == d.selectedSchema {
			return
		}
		d.selectedSchema = value
		d.schemaLabel.SetText("Schema: " + value)
		d.loadTableOverview(value)
	})
	d.schemaSelect.PlaceHolder = "Select schema/database"
	d.tableMeta = widget.NewLabel("Total tables: 0")
	refreshBtn := widget.NewButtonWithIcon("Refresh", theme.ViewRefreshIcon(), func() { d.refreshExplorer() })
	openQuery := widget.NewButtonWithIcon("Open Query Editor", theme.MediaPlayIcon(), func() {
		if d.tabs != nil {
			d.tabs.SelectIndex(2)
		}
	})
	refreshBtn.Importance = widget.LowImportance

	d.tableList = widget.NewList(
		func() int { return len(d.tableInfos) },
		func() fyne.CanvasObject {
			name := widget.NewButton("", nil)
			name.Importance = widget.LowImportance
			rows := widget.NewLabel("")
			size := widget.NewLabel("")
			updated := widget.NewLabel("")
			return container.NewGridWithColumns(4, name, rows, size, updated)
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			if id < 0 || id >= len(d.tableInfos) {
				return
			}
			item := d.tableInfos[id]
			row := obj.(*fyne.Container)
			name := row.Objects[0].(*widget.Button)
			rows := row.Objects[1].(*widget.Label)
			size := row.Objects[2].(*widget.Label)
			updated := row.Objects[3].(*widget.Label)
			name.SetText(item.Name)
			name.OnTapped = func() {
				d.openTable(item.Schema, item.Name)
			}
			rows.SetText(formatInt(item.Rows))
			size.SetText(item.Size)
			updated.SetText(item.LastUpdated)
		},
	)

	leftHeader := container.NewVBox(
		widget.NewLabelWithStyle("Objects", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabel("Schemas, tables, views, procedures and triggers"),
	)
	left := d.panel(container.NewBorder(leftHeader, nil, nil, nil, d.explorerTree))

	columnHeader := container.NewGridWithColumns(
		4,
		widget.NewLabelWithStyle("TABLE NAME", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabelWithStyle("ROW COUNT", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabelWithStyle("SIZE", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabelWithStyle("LAST UPDATED", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
	)
	rightHeader := container.NewBorder(
		nil,
		nil,
		container.NewVBox(
			widget.NewLabelWithStyle("Tables Overview", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
			container.NewHBox(widget.NewLabel("Database"), d.hGap(8), d.fixed(d.schemaSelect, 210, 34)),
			d.schemaLabel,
			d.tableMeta,
		),
		container.NewVBox(
			container.NewHBox(
				d.fixed(refreshBtn, 88, 30),
				d.hGap(8),
				d.fixed(openQuery, 132, 30),
			),
			layout.NewSpacer(),
		),
		nil,
	)
	right := d.panel(container.NewBorder(
		container.NewVBox(rightHeader, widget.NewSeparator(), columnHeader),
		nil,
		nil,
		nil,
		d.tableList,
	))

	split := container.NewHSplit(left, right)
	split.Offset = 0.32
	return split
}

func (d *DesktopApp) buildTableDataView() fyne.CanvasObject {
	d.tableLabel = widget.NewLabelWithStyle("Table: -", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	d.summary = widget.NewLabel("No table selected")
	d.pageLabel = widget.NewLabel("Page 1")
	d.filterEntry = widget.NewEntry()
	d.filterEntry.SetPlaceHolder("Filter (SQL WHERE / Mongo JSON / Redis pattern)")
	d.sortEntry = widget.NewEntry()
	d.sortEntry.SetPlaceHolder("Sort expression")
	d.limitEntry = widget.NewEntry()
	d.limitEntry.SetText("50")
	breadcrumb := widget.NewLabel("Explorer > Schema > Table")

	d.filterEntry.OnSubmitted = func(string) {
		d.page = 1
		d.refreshTableData()
	}
	d.sortEntry.OnSubmitted = func(string) {
		d.sortColumn = ""
		d.sortDesc = false
		d.page = 1
		d.refreshTableData()
	}
	d.limitEntry.OnSubmitted = func(string) {
		d.page = 1
		d.refreshTableData()
	}

	d.openTableTabs = container.NewAppTabs()
	d.openTableTabs.SetTabLocation(container.TabLocationTop)
	d.openTableTabs.OnSelected = func(item *container.TabItem) {
		if d.syncingTableTabs || item == nil {
			return
		}
		index := indexOfOpenTableByKey(d.openTables, item.Text)
		if index >= 0 {
			d.selectOpenTable(index)
		}
	}

	d.openTableList = widget.NewList(
		func() int { return len(d.openTables) },
		func() fyne.CanvasObject {
			return widget.NewLabelWithStyle("", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			if id < 0 || id >= len(d.openTables) {
				return
			}
			item := d.openTables[id]
			obj.(*widget.Label).SetText(tableKey(item.Schema, item.Name))
		},
	)
	d.openTableList.OnSelected = func(id widget.ListItemID) {
		if d.syncingTableTabs {
			return
		}
		d.selectOpenTable(id)
	}

	refresh := widget.NewButtonWithIcon("Refresh", theme.ViewRefreshIcon(), func() {
		d.page = 1
		d.refreshTableData()
	})
	insert := widget.NewButtonWithIcon("New", theme.ContentAddIcon(), func() { d.showInsertDialog() })
	update := widget.NewButtonWithIcon("Update", theme.DocumentCreateIcon(), func() { d.showUpdateDialog() })
	remove := widget.NewButtonWithIcon("Delete", theme.DeleteIcon(), func() { d.showDeleteDialog() })
	export := widget.NewButtonWithIcon("Export", theme.DownloadIcon(), func() {})
	insert.Importance = widget.HighImportance

	d.pagePrevBtn = widget.NewButtonWithIcon("Prev", theme.NavigateBackIcon(), func() {
		if d.page > 1 {
			d.page--
			d.refreshTableData()
		}
	})
	d.pageNextBtn = widget.NewButtonWithIcon("Next", theme.NavigateNextIcon(), func() {
		if d.canGoToNextPage() {
			d.page++
			d.refreshTableData()
		}
	})

	titleRow := container.NewBorder(
		nil,
		nil,
		container.NewVBox(breadcrumb, d.tableLabel),
		container.NewHBox(d.fixed(export, 88, 30), d.hGap(6), d.fixed(insert, 88, 30)),
		nil,
	)
	filterControls := container.NewBorder(
		nil,
		nil,
		nil,
		d.fixed(labeledField("Rows", d.limitEntry), 92, 54),
		container.NewGridWithColumns(
			2,
			labeledField("Filter", d.filterEntry),
			labeledField("Sort", d.sortEntry),
		),
	)
	actionControls := container.NewHBox(
		d.fixed(refresh, 82, 30),
		d.hGap(6),
		d.fixed(update, 82, 30),
		d.hGap(6),
		d.fixed(remove, 82, 30),
	)
	filterRow := container.NewBorder(nil, nil, filterControls, actionControls, nil)

	d.tableGrid = widget.NewTable(
		func() (int, int) {
			if len(d.tableData.Columns) == 0 {
				return 1, 1
			}
			return len(d.tableData.Rows) + 1, len(d.tableData.Columns)
		},
		func() fyne.CanvasObject {
			label := widget.NewLabel("")
			label.Truncation = fyne.TextTruncateEllipsis
			return label
		},
		func(cell widget.TableCellID, obj fyne.CanvasObject) {
			label := obj.(*widget.Label)
			if len(d.tableData.Columns) == 0 {
				label.SetText("No data loaded")
				return
			}
			if cell.Row == 0 {
				if cell.Col < len(d.tableData.Columns) {
					col := d.tableData.Columns[cell.Col]
					header := col + " ↕"
					if d.sortColumn == col {
						if d.sortDesc {
							header = col + " ▼"
						} else {
							header = col + " ▲"
						}
					}
					label.TextStyle = fyne.TextStyle{Bold: true}
					label.SetText(header)
				}
				return
			}
			label.TextStyle = fyne.TextStyle{}
			row := cell.Row - 1
			if row < len(d.tableData.Rows) && cell.Col < len(d.tableData.Rows[row]) {
				label.SetText(d.tableData.Rows[row][cell.Col])
				return
			}
			label.SetText("")
		},
	)
	d.tableGrid.OnSelected = func(id widget.TableCellID) {
		if id.Row == 0 {
			d.toggleColumnSort(id.Col)
			return
		}
		if id.Row > 0 {
			d.selectedDataRow = id.Row - 1
		}
	}

	pager := container.NewHBox(
		d.fixed(d.pagePrevBtn, 88, 30),
		d.hGap(6),
		d.fixed(d.pageNextBtn, 88, 30),
		d.hGap(10),
		d.pageLabel,
		layout.NewSpacer(),
	)

	rightContent := container.NewBorder(
		container.NewVBox(titleRow, d.openTableTabs, filterRow, d.summary, widget.NewSeparator()),
		pager,
		nil,
		nil,
		d.panel(d.tableGrid),
	)

	leftContent := d.panel(container.NewBorder(
		widget.NewLabelWithStyle("Opened Tables", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		nil,
		nil,
		nil,
		d.fixed(d.openTableList, 220, 380),
	))

	split := container.NewHSplit(leftContent, rightContent)
	split.Offset = 0.18
	d.refreshOpenTableViews()
	return split
}

func (d *DesktopApp) refreshOpenTableViews() {
	if d.openTableTabs != nil {
		items := make([]*container.TabItem, 0, len(d.openTables))
		selectedKey := tableKey(d.selectedSchema, d.selectedTable)
		selectedTab := -1
		for i, open := range d.openTables {
			key := tableKey(open.Schema, open.Name)
			items = append(items, container.NewTabItem(key, widget.NewLabel("")))
			if key == selectedKey {
				selectedTab = i
			}
		}
		d.syncingTableTabs = true
		d.openTableTabs.SetItems(items)
		if selectedTab >= 0 && selectedTab < len(items) {
			d.openTableTabs.SelectIndex(selectedTab)
		}
		d.syncingTableTabs = false
	}

	if d.openTableList != nil {
		for i := range d.openTables {
			d.openTableList.SetItemHeight(i, 36)
		}
		d.openTableList.Refresh()

		selectedList := indexOfOpenTableByKey(d.openTables, tableKey(d.selectedSchema, d.selectedTable))
		d.syncingTableTabs = true
		if selectedList >= 0 && selectedList < len(d.openTables) {
			d.openTableList.Select(selectedList)
			d.openTableList.ScrollTo(selectedList)
		} else {
			d.openTableList.UnselectAll()
		}
		d.syncingTableTabs = false
	}
}

func (d *DesktopApp) selectOpenTable(index int) {
	if index < 0 || index >= len(d.openTables) {
		return
	}
	selected := d.openTables[index]
	changed := d.selectedSchema != selected.Schema || d.selectedTable != selected.Name
	d.selectedSchema = selected.Schema
	d.selectedTable = selected.Name
	if changed {
		d.page = 1
		d.sortColumn = ""
		d.sortDesc = false
		if d.filterEntry != nil {
			d.filterEntry.SetText("")
		}
		if d.sortEntry != nil {
			d.sortEntry.SetText("")
		}
	}
	if d.tableLabel != nil {
		d.tableLabel.SetText(tableKey(selected.Schema, selected.Name))
	}
	d.refreshOpenTableViews()
	if changed {
		d.refreshTableData()
	}
}

func (d *DesktopApp) toggleColumnSort(colIndex int) {
	if colIndex < 0 || colIndex >= len(d.tableData.Columns) {
		return
	}
	column := d.tableData.Columns[colIndex]
	if d.sortColumn == column {
		d.sortDesc = !d.sortDesc
	} else {
		d.sortColumn = column
		d.sortDesc = false
	}

	conn, ok := d.currentActiveConnection()
	if !ok {
		return
	}
	switch conn.Type {
	case model.DBTypeMongo:
		direction := 1
		if d.sortDesc {
			direction = -1
		}
		d.sortEntry.SetText(fmt.Sprintf("{%q: %d}", column, direction))
	case model.DBTypeRedis:
		if d.sortDesc {
			d.sortEntry.SetText(column + " DESC")
		} else {
			d.sortEntry.SetText(column + " ASC")
		}
	default:
		if d.sortDesc {
			d.sortEntry.SetText(column + " DESC")
		} else {
			d.sortEntry.SetText(column + " ASC")
		}
	}

	d.page = 1
	d.refreshTableData()
}

func (d *DesktopApp) canGoToNextPage() bool {
	if d.totalRows > 0 {
		limit := d.pageSize()
		return int64(d.page*limit) < d.totalRows
	}
	return d.hasNextPage
}

func (d *DesktopApp) pageSize() int {
	limit := 50
	if d.limitEntry == nil {
		return limit
	}
	value := strings.TrimSpace(d.limitEntry.Text)
	if value == "" {
		d.limitEntry.SetText(strconv.Itoa(limit))
		return limit
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		d.limitEntry.SetText(strconv.Itoa(limit))
		return limit
	}
	if parsed > 1000 {
		parsed = 1000
		d.limitEntry.SetText("1000")
	}
	return parsed
}

func (d *DesktopApp) updatePagerState(totalRows int64) {
	if d.page < 1 {
		d.page = 1
	}

	if totalRows < 0 {
		if d.pageLabel != nil {
			d.pageLabel.SetText(fmt.Sprintf("Page %d", d.page))
		}
		if d.pagePrevBtn != nil {
			if d.page > 1 {
				d.pagePrevBtn.Enable()
			} else {
				d.pagePrevBtn.Disable()
			}
		}
		if d.pageNextBtn != nil {
			if d.hasNextPage {
				d.pageNextBtn.Enable()
			} else {
				d.pageNextBtn.Disable()
			}
		}
		return
	}

	limit := d.pageSize()
	totalPages := 1
	if totalRows > 0 {
		totalPages = int((totalRows + int64(limit) - 1) / int64(limit))
		if totalPages < 1 {
			totalPages = 1
		}
	}
	if d.page > totalPages {
		d.page = totalPages
	}

	if d.pageLabel != nil {
		d.pageLabel.SetText(fmt.Sprintf("Page %d of %d", d.page, totalPages))
	}
	if d.pagePrevBtn != nil {
		if d.page > 1 {
			d.pagePrevBtn.Enable()
		} else {
			d.pagePrevBtn.Disable()
		}
	}
	if d.pageNextBtn != nil {
		if int64(d.page*limit) < totalRows {
			d.pageNextBtn.Enable()
		} else {
			d.pageNextBtn.Disable()
		}
	}
}

func (d *DesktopApp) buildQueryView() fyne.CanvasObject {
	d.queryEditor = widget.NewMultiLineEntry()
	d.queryEditor.SetPlaceHolder("Write SQL / Mongo command JSON / Redis command...")
	d.queryEditor.Wrapping = fyne.TextWrapWord

	runBtn := widget.NewButtonWithIcon("Run Selection", theme.MediaPlayIcon(), func() { d.runQuery() })
	saveBtn := widget.NewButtonWithIcon("Save Snippet", theme.DocumentSaveIcon(), func() { d.showSaveSnippetDialog() })
	formatBtn := widget.NewButtonWithIcon("Format SQL", theme.DocumentCreateIcon(), func() {})
	clearBtn := widget.NewButtonWithIcon("Clear", theme.ContentClearIcon(), func() { d.queryEditor.SetText("") })
	runBtn.Importance = widget.HighImportance
	d.queryMessage = widget.NewLabel("Ready")
	toolbar := container.NewBorder(
		nil,
		nil,
		container.NewHBox(runBtn, formatBtn, saveBtn, clearBtn),
		nil,
		d.queryMessage,
	)

	d.resultsTable = widget.NewTable(
		func() (int, int) {
			if len(d.queryResult.Columns) == 0 {
				return 1, 1
			}
			return len(d.queryResult.Rows) + 1, len(d.queryResult.Columns)
		},
		func() fyne.CanvasObject {
			label := widget.NewLabel("")
			label.Truncation = fyne.TextTruncateEllipsis
			return label
		},
		func(cell widget.TableCellID, obj fyne.CanvasObject) {
			label := obj.(*widget.Label)
			if len(d.queryResult.Columns) == 0 {
				label.SetText("Run a query to see results")
				return
			}
			if cell.Row == 0 {
				if cell.Col < len(d.queryResult.Columns) {
					label.TextStyle = fyne.TextStyle{Bold: true}
					label.SetText(d.queryResult.Columns[cell.Col])
				}
				return
			}
			label.TextStyle = fyne.TextStyle{}
			row := cell.Row - 1
			if row < len(d.queryResult.Rows) && cell.Col < len(d.queryResult.Rows[row]) {
				label.SetText(d.queryResult.Rows[row][cell.Col])
			} else {
				label.SetText("")
			}
		},
	)

	editorResultSplit := container.NewVSplit(d.panel(d.queryEditor), d.panel(d.resultsTable))
	editorResultSplit.Offset = 0.55
	right := container.NewBorder(toolbar, nil, nil, nil, editorResultSplit)

	d.snippetList = widget.NewList(
		func() int { return len(d.snippets) },
		func() fyne.CanvasObject {
			name := widget.NewLabelWithStyle("", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
			query := widget.NewLabel("")
			query.Truncation = fyne.TextTruncateEllipsis
			return container.NewVBox(name, query)
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			if id < 0 || id >= len(d.snippets) {
				return
			}
			item := d.snippets[id]
			box := obj.(*fyne.Container)
			box.Objects[0].(*widget.Label).SetText(item.Name)
			box.Objects[1].(*widget.Label).SetText(compactText(item.Query, 80))
		},
	)
	d.snippetList.OnSelected = func(id widget.ListItemID) {
		if id >= 0 && id < len(d.snippets) {
			d.queryEditor.SetText(d.snippets[id].Query)
		}
	}

	d.historyList = widget.NewList(
		func() int { return len(d.history) },
		func() fyne.CanvasObject {
			query := widget.NewLabelWithStyle("", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
			meta := widget.NewLabel("")
			return container.NewVBox(query, meta)
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			if id < 0 || id >= len(d.history) {
				return
			}
			item := d.history[id]
			box := obj.(*fyne.Container)
			box.Objects[0].(*widget.Label).SetText(compactText(item.Query, 70))
			status := "ok"
			if item.Error != "" {
				status = "error"
			}
			box.Objects[1].(*widget.Label).SetText(fmt.Sprintf("%s | %dms | %s", item.ExecutedAt.Format("2006-01-02 15:04:05"), item.DurationMs, status))
		},
	)
	d.historyList.OnSelected = func(id widget.ListItemID) {
		if id >= 0 && id < len(d.history) {
			d.queryEditor.SetText(d.history[id].Query)
		}
	}

	d.queryDB = widget.NewLabel("No active database")
	sideTabs := container.NewAppTabs(
		container.NewTabItem("Snippets", d.snippetList),
		container.NewTabItem("History", d.historyList),
	)
	sideHeader := container.NewVBox(
		widget.NewLabelWithStyle("ACTIVE DATABASE", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		d.queryDB,
		widget.NewSeparator(),
	)
	side := d.panel(container.NewBorder(sideHeader, nil, nil, nil, sideTabs))
	d.syncConnectionHeader()

	split := container.NewHSplit(side, right)
	split.Offset = 0.28
	return split
}

func (d *DesktopApp) openConnectionAt(index int) {
	if index < 0 || index >= len(d.connections) {
		return
	}
	conn := d.connections[index]
	d.selectedConnection = conn.ID
	d.connStatus.SetText("Connecting: " + conn.Name)

	if err := d.dbService.Connect(conn); err != nil {
		d.connStatus.SetText("Connection failed")
		d.alertError(err)
		return
	}
	d.activeConnectionID = conn.ID
	d.connStatus.SetText("Connected: " + conn.Name)
	d.openTables = nil
	d.selectedSchema = ""
	d.selectedTable = ""
	d.sortColumn = ""
	d.sortDesc = false
	d.page = 1
	d.totalRows = 0
	d.hasNextPage = false
	d.tableData = model.TableData{}
	d.selectedDataRow = -1
	if d.filterEntry != nil {
		d.filterEntry.SetText("")
	}
	if d.sortEntry != nil {
		d.sortEntry.SetText("")
	}
	if d.limitEntry != nil {
		d.limitEntry.SetText("50")
	}
	d.refreshOpenTableViews()
	d.syncConnectionHeader()
	d.refreshExplorer()
	if d.tabs != nil {
		d.tabs.SelectIndex(0)
	}
	d.refreshAllLists()
}

func (d *DesktopApp) refreshExplorer() {
	if d.activeConnectionID == "" {
		return
	}
	objects, err := d.dbService.ListObjects(d.activeConnectionID)
	if err != nil {
		d.alertError(err)
		return
	}

	defaultSchema := d.selectedSchema
	if defaultSchema == "" {
		defaultSchema = firstSchema(objects)
	}

	d.objects = objects
	d.updateSchemaOptions()
	d.rebuildTree()
	d.loadTableOverview(defaultSchema)
}

func (d *DesktopApp) loadTableOverview(schema string) {
	if d.activeConnectionID == "" {
		return
	}
	if schema == "" {
		schema = d.selectedSchema
	}
	d.selectedSchema = schema
	d.schemaLabel.SetText("Schema: " + schema)

	rows, err := d.dbService.ListTableInfo(d.activeConnectionID, schema)
	if err != nil {
		d.alertError(err)
		return
	}
	if schema == "" && len(rows) > 0 {
		schema = rows[0].Schema
		d.selectedSchema = schema
		d.schemaLabel.SetText("Schema: " + schema)
	}
	if len(d.objects) == 0 && len(rows) > 0 {
		schemaName := schema
		if schemaName == "" {
			schemaName = rows[0].Schema
		}
		synthetic := []model.DBObject{{Schema: schemaName, Name: schemaName, Type: "schema"}}
		for _, row := range rows {
			synthetic = append(synthetic, model.DBObject{Schema: row.Schema, Name: row.Name, Type: "table"})
		}
		d.objects = synthetic
		d.rebuildTree()
	}
	d.tableInfosAll = rows
	d.updateSchemaOptions()
	d.applyTableFilter()
}

func (d *DesktopApp) applyTableFilter() {
	if strings.TrimSpace(d.globalSearch) == "" {
		d.tableInfos = append([]model.TableInfo(nil), d.tableInfosAll...)
	} else {
		filtered := make([]model.TableInfo, 0, len(d.tableInfosAll))
		for _, row := range d.tableInfosAll {
			if strings.Contains(strings.ToLower(row.Name), d.globalSearch) || strings.Contains(strings.ToLower(row.Schema), d.globalSearch) {
				filtered = append(filtered, row)
			}
		}
		d.tableInfos = filtered
	}
	if d.tableMeta != nil {
		totalRows := int64(0)
		for _, item := range d.tableInfos {
			totalRows += item.Rows
		}
		d.tableMeta.SetText(fmt.Sprintf("Total tables: %d | Rows: %s", len(d.tableInfos), formatInt(totalRows)))
	}
	if d.tableList == nil {
		return
	}
	for i := range d.tableInfos {
		d.tableList.SetItemHeight(i, 42)
	}
	d.tableList.Refresh()
}

func (d *DesktopApp) rebuildTree() {
	children := map[string][]string{"root": {}}
	labels := map[string]string{"root": "Objects"}

	schemaNodes := map[string]string{}
	for _, item := range d.objects {
		if item.Type == "schema" || item.Type == "database" {
			nodeID := "schema|" + item.Name
			if _, ok := schemaNodes[item.Name]; !ok {
				schemaNodes[item.Name] = nodeID
				children["root"] = append(children["root"], nodeID)
				labels[nodeID] = item.Name
				children[nodeID] = []string{}
			}
		}
	}

	for _, item := range d.objects {
		if item.Type == "schema" || item.Type == "database" {
			continue
		}
		schema := item.Schema
		if schema == "" {
			schema = "default"
		}
		nodeID, ok := schemaNodes[schema]
		if !ok {
			nodeID = "schema|" + schema
			schemaNodes[schema] = nodeID
			children["root"] = append(children["root"], nodeID)
			labels[nodeID] = schema
			children[nodeID] = []string{}
		}

		objID := fmt.Sprintf("obj|%s|%s|%s", schema, item.Type, item.Name)
		children[nodeID] = append(children[nodeID], objID)
		labels[objID] = fmt.Sprintf("%s (%s)", item.Name, item.Type)
	}

	sort.Strings(children["root"])
	for key := range children {
		if key == "root" {
			continue
		}
		sort.Strings(children[key])
	}

	if len(children["root"]) == 0 && strings.TrimSpace(d.selectedSchema) != "" {
		fallbackNode := "schema|" + d.selectedSchema
		children["root"] = append(children["root"], fallbackNode)
		children[fallbackNode] = []string{}
		labels[fallbackNode] = d.selectedSchema
	}

	if len(children["root"]) == 0 {
		emptyNode := "empty|no_schemas"
		children["root"] = append(children["root"], emptyNode)
		children[emptyNode] = []string{}
		labels[emptyNode] = "No schemas/databases found"
	}

	d.treeChildren = children
	d.treeLabel = labels
	d.explorerTree.Refresh()
	d.explorerTree.OpenBranch("root")
	for _, schemaID := range children["root"] {
		d.explorerTree.OpenBranch(schemaID)
	}
}

func (d *DesktopApp) openTable(schema, table string) {
	if strings.TrimSpace(table) == "" {
		return
	}
	if schema == "" {
		schema = d.selectedSchema
	}

	target := openTableEntry{Schema: schema, Name: table}
	openIndex := indexOfOpenTable(d.openTables, target)
	if openIndex < 0 {
		d.openTables = append(d.openTables, target)
	}

	changedTable := d.selectedSchema != schema || d.selectedTable != table
	d.selectedSchema = schema
	d.selectedTable = table
	if changedTable {
		d.page = 1
		d.sortColumn = ""
		d.sortDesc = false
		if d.filterEntry != nil {
			d.filterEntry.SetText("")
		}
		if d.sortEntry != nil {
			d.sortEntry.SetText("")
		}
	}
	d.tableLabel.SetText(tableKey(schema, table))
	d.refreshOpenTableViews()
	d.tabs.SelectIndex(1)
	d.refreshTableData()
}

func (d *DesktopApp) refreshTableData() {
	if d.activeConnectionID == "" {
		d.tableData = model.TableData{}
		d.totalRows = 0
		d.hasNextPage = false
		if d.summary != nil {
			d.summary.SetText("No active connection")
		}
		if d.tableGrid != nil {
			d.tableGrid.Refresh()
		}
		d.updatePagerState(0)
		return
	}
	if d.selectedTable == "" {
		d.tableData = model.TableData{}
		d.totalRows = 0
		d.hasNextPage = false
		if d.summary != nil {
			d.summary.SetText("Select a table to view rows")
		}
		if d.tableGrid != nil {
			d.tableGrid.Refresh()
		}
		d.updatePagerState(0)
		return
	}
	limit := d.pageSize()
	filter := strings.TrimSpace(d.filterEntry.Text)
	sortBy := strings.TrimSpace(d.sortEntry.Text)
	fetchLimit := limit + 1

	var data model.TableData
	var hasNext bool

	for {
		offset := (d.page - 1) * limit
		if offset < 0 {
			offset = 0
		}

		nextData, err := d.dbService.FetchTableData(d.activeConnectionID, d.selectedSchema, d.selectedTable, filter, sortBy, fetchLimit, offset)
		if err != nil {
			d.alertError(err)
			return
		}

		hasNext = len(nextData.Rows) > limit
		if hasNext {
			nextData.Rows = nextData.Rows[:limit]
		}
		if d.page > 1 && len(nextData.Rows) == 0 {
			d.page--
			continue
		}
		data = nextData
		break
	}

	d.totalRows = -1
	d.hasNextPage = hasNext
	d.tableData = data
	d.selectedDataRow = -1
	offset := (d.page - 1) * limit
	if len(data.Rows) == 0 {
		d.summary.SetText("No rows found")
	} else {
		start := int64(offset + 1)
		end := int64(offset + len(data.Rows))
		d.summary.SetText(fmt.Sprintf("Showing %s-%s rows", formatInt(start), formatInt(end)))
	}

	d.updatePagerState(-1)
	d.tableGrid.Refresh()
	for col := range d.tableData.Columns {
		d.tableGrid.SetColumnWidth(col, 180)
	}
}

func (d *DesktopApp) showInsertDialog() {
	conn, ok := d.currentActiveConnection()
	if !ok {
		d.alertError(fmt.Errorf("connect to a database first"))
		return
	}

	switch conn.Type {
	case model.DBTypeRedis:
		d.showRedisInsertDialog()
	case model.DBTypeMongo:
		d.showMongoInsertDialog()
	default:
		d.showSQLRowDialog("Insert", func(row map[string]string, keyCol, keyVal string) error {
			return d.dbService.InsertRow(d.activeConnectionID, d.selectedSchema, d.selectedTable, row)
		})
	}
}

func (d *DesktopApp) showUpdateDialog() {
	conn, ok := d.currentActiveConnection()
	if !ok {
		d.alertError(fmt.Errorf("connect to a database first"))
		return
	}

	switch conn.Type {
	case model.DBTypeRedis:
		d.showRedisUpdateDialog()
	case model.DBTypeMongo:
		d.showMongoUpdateDialog()
	default:
		d.showSQLRowDialog("Update", func(row map[string]string, keyCol, keyVal string) error {
			return d.dbService.UpdateRow(d.activeConnectionID, d.selectedSchema, d.selectedTable, keyCol, keyVal, row)
		})
	}
}

func (d *DesktopApp) showDeleteDialog() {
	conn, ok := d.currentActiveConnection()
	if !ok {
		d.alertError(fmt.Errorf("connect to a database first"))
		return
	}

	switch conn.Type {
	case model.DBTypeRedis:
		d.showRedisDeleteDialog()
	default:
		d.showDeleteByKeyDialog()
	}
}

func (d *DesktopApp) showSQLRowDialog(title string, submit func(row map[string]string, keyCol, keyVal string) error) {
	if d.selectedTable == "" {
		d.alertError(fmt.Errorf("select a table first"))
		return
	}
	if len(d.tableData.Columns) == 0 {
		d.alertError(fmt.Errorf("load table data first"))
		return
	}

	entries := map[string]*widget.Entry{}
	formItems := make([]*widget.FormItem, 0, len(d.tableData.Columns)+2)
	for _, col := range d.tableData.Columns {
		entry := widget.NewEntry()
		if d.selectedDataRow >= 0 && d.selectedDataRow < len(d.tableData.Rows) {
			index := indexOf(d.tableData.Columns, col)
			if index >= 0 && index < len(d.tableData.Rows[d.selectedDataRow]) {
				entry.SetText(d.tableData.Rows[d.selectedDataRow][index])
			}
		}
		entries[col] = entry
		formItems = append(formItems, widget.NewFormItem(col, entry))
	}

	keyColumn := widget.NewSelect(d.tableData.Columns, nil)
	if len(d.tableData.Columns) > 0 {
		keyColumn.SetSelected(d.tableData.Columns[0])
	}
	keyValue := widget.NewEntry()
	if d.selectedDataRow >= 0 && d.selectedDataRow < len(d.tableData.Rows) && len(d.tableData.Columns) > 0 {
		keyValue.SetText(d.tableData.Rows[d.selectedDataRow][0])
	}
	if title == "Update" {
		formItems = append(formItems, widget.NewFormItem("Key Column", keyColumn))
		formItems = append(formItems, widget.NewFormItem("Key Value", keyValue))
	}

	form := widget.NewForm(formItems...)
	form.SubmitText = title
	form.CancelText = "Cancel"
	form.OnSubmit = func() {
		payload := map[string]string{}
		for col, entry := range entries {
			payload[col] = entry.Text
		}
		err := submit(payload, keyColumn.Selected, keyValue.Text)
		if err != nil {
			d.alertError(err)
			return
		}
		d.refreshTableData()
	}
	form.OnCancel = func() {}

	dlg := dialog.NewCustom(title+" Row", "Close", container.NewVScroll(form), d.win)
	dlg.Resize(fyne.NewSize(560, 600))
	dlg.Show()
}

func (d *DesktopApp) showDeleteByKeyDialog() {
	if d.selectedTable == "" {
		d.alertError(fmt.Errorf("select a table first"))
		return
	}
	if len(d.tableData.Columns) == 0 {
		d.alertError(fmt.Errorf("load table data first"))
		return
	}

	keyColumn := widget.NewSelect(d.tableData.Columns, nil)
	if len(d.tableData.Columns) > 0 {
		keyColumn.SetSelected(d.tableData.Columns[0])
	}
	keyValue := widget.NewEntry()
	if d.selectedDataRow >= 0 && d.selectedDataRow < len(d.tableData.Rows) && len(d.tableData.Columns) > 0 {
		keyValue.SetText(d.tableData.Rows[d.selectedDataRow][0])
	}

	content := widget.NewForm(
		widget.NewFormItem("Key Column", keyColumn),
		widget.NewFormItem("Key Value", keyValue),
	)
	dialog.ShowCustomConfirm("Delete Row", "Delete", "Cancel", content, func(confirm bool) {
		if !confirm {
			return
		}
		err := d.dbService.DeleteRow(d.activeConnectionID, d.selectedSchema, d.selectedTable, keyColumn.Selected, keyValue.Text)
		if err != nil {
			d.alertError(err)
			return
		}
		d.refreshTableData()
	}, d.win)
}

func (d *DesktopApp) showMongoInsertDialog() {
	doc := widget.NewMultiLineEntry()
	doc.SetPlaceHolder(`{"name":"value"}`)
	content := widget.NewForm(widget.NewFormItem("Document JSON", doc))
	dialog.ShowCustomConfirm("Insert Document", "Insert", "Cancel", content, func(confirm bool) {
		if !confirm {
			return
		}
		err := d.dbService.InsertRow(d.activeConnectionID, d.selectedSchema, d.selectedTable, map[string]string{"document": doc.Text})
		if err != nil {
			d.alertError(err)
			return
		}
		d.refreshTableData()
	}, d.win)
}

func (d *DesktopApp) showMongoUpdateDialog() {
	keyCol := widget.NewEntry()
	keyCol.SetPlaceHolder("_id")
	keyVal := widget.NewEntry()
	doc := widget.NewMultiLineEntry()
	doc.SetPlaceHolder(`{"field":"new value"}`)
	content := widget.NewForm(
		widget.NewFormItem("Key Column", keyCol),
		widget.NewFormItem("Key Value", keyVal),
		widget.NewFormItem("Set JSON", doc),
	)
	dialog.ShowCustomConfirm("Update Document", "Update", "Cancel", content, func(confirm bool) {
		if !confirm {
			return
		}
		if strings.TrimSpace(keyCol.Text) == "" {
			keyCol.SetText("_id")
		}
		err := d.dbService.UpdateRow(d.activeConnectionID, d.selectedSchema, d.selectedTable, keyCol.Text, keyVal.Text, map[string]string{"document": doc.Text})
		if err != nil {
			d.alertError(err)
			return
		}
		d.refreshTableData()
	}, d.win)
}

func (d *DesktopApp) showRedisInsertDialog() {
	key := widget.NewEntry()
	value := widget.NewEntry()
	content := widget.NewForm(
		widget.NewFormItem("Key", key),
		widget.NewFormItem("Value", value),
	)
	dialog.ShowCustomConfirm("Insert Redis Value", "Insert", "Cancel", content, func(confirm bool) {
		if !confirm {
			return
		}
		err := d.dbService.InsertRow(d.activeConnectionID, "", d.selectedTable, map[string]string{"key": key.Text, "value": value.Text})
		if err != nil {
			d.alertError(err)
			return
		}
		d.refreshTableData()
	}, d.win)
}

func (d *DesktopApp) showRedisUpdateDialog() {
	key := widget.NewEntry()
	value := widget.NewEntry()
	if d.selectedDataRow >= 0 && d.selectedDataRow < len(d.tableData.Rows) {
		row := d.tableData.Rows[d.selectedDataRow]
		if len(row) > 0 {
			key.SetText(row[0])
		}
		if len(row) > 2 {
			value.SetText(row[2])
		}
	}
	content := widget.NewForm(
		widget.NewFormItem("Key", key),
		widget.NewFormItem("Value", value),
	)
	dialog.ShowCustomConfirm("Update Redis Value", "Update", "Cancel", content, func(confirm bool) {
		if !confirm {
			return
		}
		err := d.dbService.UpdateRow(d.activeConnectionID, "", d.selectedTable, "key", key.Text, map[string]string{"key": key.Text, "value": value.Text})
		if err != nil {
			d.alertError(err)
			return
		}
		d.refreshTableData()
	}, d.win)
}

func (d *DesktopApp) showRedisDeleteDialog() {
	key := widget.NewEntry()
	if d.selectedDataRow >= 0 && d.selectedDataRow < len(d.tableData.Rows) {
		row := d.tableData.Rows[d.selectedDataRow]
		if len(row) > 0 {
			key.SetText(row[0])
		}
	}
	content := widget.NewForm(widget.NewFormItem("Key", key))
	dialog.ShowCustomConfirm("Delete Redis Key", "Delete", "Cancel", content, func(confirm bool) {
		if !confirm {
			return
		}
		err := d.dbService.DeleteRow(d.activeConnectionID, "", d.selectedTable, "key", key.Text)
		if err != nil {
			d.alertError(err)
			return
		}
		d.refreshTableData()
	}, d.win)
}

func (d *DesktopApp) runQuery() {
	if d.activeConnectionID == "" {
		d.alertError(fmt.Errorf("no active connection"))
		return
	}
	query := strings.TrimSpace(d.queryEditor.Text)
	if query == "" {
		d.alertError(fmt.Errorf("query is empty"))
		return
	}
	conn, ok := d.currentActiveConnection()
	if !ok {
		d.alertError(fmt.Errorf("active connection not found"))
		return
	}

	d.queryMessage.SetText("Running...")
	result, duration, err := d.dbService.RunQuery(d.activeConnectionID, query)
	historyItem := model.QueryHistory{
		ConnectionID: d.activeConnectionID,
		Type:         conn.Type,
		Query:        query,
		DurationMs:   duration.Milliseconds(),
		ExecutedAt:   time.Now(),
	}
	if err != nil {
		historyItem.Error = err.Error()
	}
	_ = d.repo.AppendHistory(historyItem)

	d.history = d.repo.History()
	d.historyList.Refresh()
	if err != nil {
		d.queryMessage.SetText("Error")
		d.alertError(err)
		return
	}
	d.queryResult = result
	d.resultsTable.Refresh()
	for i := range d.queryResult.Columns {
		d.resultsTable.SetColumnWidth(i, 180)
	}
	d.queryMessage.SetText(fmt.Sprintf("Done in %dms | %s", duration.Milliseconds(), result.Message))
}

func (d *DesktopApp) showSaveSnippetDialog() {
	if d.activeConnectionID == "" {
		d.alertError(fmt.Errorf("no active connection"))
		return
	}
	conn, ok := d.currentActiveConnection()
	if !ok {
		d.alertError(fmt.Errorf("active connection not found"))
		return
	}
	query := strings.TrimSpace(d.queryEditor.Text)
	if query == "" {
		d.alertError(fmt.Errorf("query is empty"))
		return
	}
	nameEntry := widget.NewEntry()
	nameEntry.SetPlaceHolder("Snippet name")
	dialog.ShowCustomConfirm("Save Snippet", "Save", "Cancel", nameEntry, func(confirm bool) {
		if !confirm {
			return
		}
		snippet := model.Snippet{
			Name:       strings.TrimSpace(nameEntry.Text),
			Connection: d.activeConnectionID,
			Type:       conn.Type,
			Query:      query,
		}
		if snippet.Name == "" {
			snippet.Name = "Snippet " + time.Now().Format("2006-01-02 15:04")
		}
		if _, err := d.repo.SaveSnippet(snippet); err != nil {
			d.alertError(err)
			return
		}
		d.snippets = d.repo.Snippets()
		d.snippetList.Refresh()
	}, d.win)
}

func (d *DesktopApp) showConnectionDialog(existing *model.Connection) {
	isEdit := existing != nil
	title := "New Connection"
	if isEdit {
		title = "Edit Connection"
	}

	name := widget.NewEntry()
	typeRadio := widget.NewRadioGroup([]string{"Postgres", "MySQL", "Mongo", "Redis"}, nil)
	typeRadio.Horizontal = true
	modeSelect := widget.NewSelect([]string{"Manual", "Connection String"}, nil)
	transport := widget.NewRadioGroup([]string{"Direct Connection", "SSH Tunnel"}, nil)
	transport.Horizontal = true

	connString := widget.NewMultiLineEntry()
	connString.SetMinRowsVisible(3)
	host := widget.NewEntry()
	port := widget.NewEntry()
	database := widget.NewEntry()
	schema := widget.NewEntry()
	username := widget.NewEntry()
	password := widget.NewPasswordEntry()

	sshHost := widget.NewEntry()
	sshPort := widget.NewEntry()
	sshUser := widget.NewEntry()
	sshAuth := widget.NewSelect([]string{"Password", "Key File"}, nil)
	sshPassword := widget.NewPasswordEntry()
	sshKeyFile := widget.NewEntry()
	sshPassphrase := widget.NewPasswordEntry()

	if isEdit {
		conn := *existing
		name.SetText(conn.Name)
		typeRadio.SetSelected(typeToLabel(conn.Type))
		if conn.UseConnString {
			modeSelect.SetSelected("Connection String")
		} else {
			modeSelect.SetSelected("Manual")
		}
		connString.SetText(conn.ConnString)
		host.SetText(conn.Host)
		if conn.Port > 0 {
			port.SetText(strconv.Itoa(conn.Port))
		}
		database.SetText(conn.Database)
		schema.SetText(conn.Schema)
		username.SetText(conn.Username)
		password.SetText(conn.Password)
		if conn.SSH.Enabled {
			transport.SetSelected("SSH Tunnel")
		} else {
			transport.SetSelected("Direct Connection")
		}
		sshHost.SetText(conn.SSH.Host)
		if conn.SSH.Port > 0 {
			sshPort.SetText(strconv.Itoa(conn.SSH.Port))
		}
		sshUser.SetText(conn.SSH.User)
		if conn.SSH.AuthType == model.SSHAuthKeyFile {
			sshAuth.SetSelected("Key File")
		} else {
			sshAuth.SetSelected("Password")
		}
		sshPassword.SetText(conn.SSH.Password)
		sshKeyFile.SetText(conn.SSH.KeyFile)
		sshPassphrase.SetText(conn.SSH.Passphrase)
	} else {
		typeRadio.SetSelected("Postgres")
		modeSelect.SetSelected("Manual")
		transport.SetSelected("Direct Connection")
		port.SetText("5432")
		sshPort.SetText("22")
		sshAuth.SetSelected("Password")
	}
	typeRadio.OnChanged = func(selected string) {
		if modeSelect.Selected != "Manual" {
			return
		}
		currentPort := strings.TrimSpace(port.Text)
		if currentPort == "" || currentPort == "5432" || currentPort == "3306" || currentPort == "27017" || currentPort == "6379" {
			port.SetText(strconv.Itoa(defaultPort(labelToType(selected))))
		}
	}

	manualForm := widget.NewForm(
		widget.NewFormItem("Host", host),
		widget.NewFormItem("Port", port),
		widget.NewFormItem("Database", database),
		widget.NewFormItem("Schema", schema),
		widget.NewFormItem("Username", username),
		widget.NewFormItem("Password", password),
	)
	connStringForm := widget.NewForm(widget.NewFormItem("Connection String", connString))
	sshForm := widget.NewForm(
		widget.NewFormItem("SSH Host", sshHost),
		widget.NewFormItem("SSH Port", sshPort),
		widget.NewFormItem("SSH User", sshUser),
		widget.NewFormItem("Auth", sshAuth),
		widget.NewFormItem("SSH Password", sshPassword),
		widget.NewFormItem("Key File", sshKeyFile),
		widget.NewFormItem("Key Passphrase", sshPassphrase),
	)
	sshPanel := d.panel(container.NewVBox(
		widget.NewLabelWithStyle("SSH Tunnel", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabel("Configure host, auth, private key and passphrase."),
		sshForm,
	))

	toggleAuthFields := func() {
		if sshAuth.Selected == "Key File" {
			sshPassword.Disable()
			sshKeyFile.Enable()
			sshPassphrase.Enable()
		} else {
			sshPassword.Enable()
			sshKeyFile.Disable()
			sshPassphrase.Disable()
		}
	}
	toggleModeFields := func() {
		if modeSelect.Selected == "Connection String" {
			manualForm.Hide()
			connStringForm.Show()
		} else {
			manualForm.Show()
			connStringForm.Hide()
		}
	}
	toggleSSHFields := func() {
		if transport.Selected == "SSH Tunnel" {
			sshPanel.Show()
		} else {
			sshPanel.Hide()
		}
	}

	sshAuth.OnChanged = func(string) { toggleAuthFields() }
	modeSelect.OnChanged = func(string) { toggleModeFields() }
	transport.OnChanged = func(string) { toggleSSHFields() }

	toggleAuthFields()
	toggleModeFields()
	toggleSSHFields()

	var formDialog dialog.Dialog

	buildConn := func() (model.Connection, error) {
		conn := model.Connection{}
		if isEdit {
			conn.ID = existing.ID
			conn.CreatedAt = existing.CreatedAt
		}
		conn.Name = strings.TrimSpace(name.Text)
		conn.Type = labelToType(typeRadio.Selected)
		conn.UseConnString = modeSelect.Selected == "Connection String"
		conn.ConnString = strings.TrimSpace(connString.Text)
		conn.Host = strings.TrimSpace(host.Text)
		conn.Database = strings.TrimSpace(database.Text)
		conn.Schema = strings.TrimSpace(schema.Text)
		conn.Username = strings.TrimSpace(username.Text)
		conn.Password = password.Text

		if conn.Name == "" {
			return conn, fmt.Errorf("connection name is required")
		}
		if conn.UseConnString {
			if conn.ConnString == "" {
				return conn, fmt.Errorf("connection string is required")
			}
		} else {
			if conn.Host == "" {
				return conn, fmt.Errorf("host is required in manual mode")
			}
			parsedPort, err := parsePort(port.Text, defaultPort(conn.Type))
			if err != nil {
				return conn, fmt.Errorf("invalid db port: %w", err)
			}
			conn.Port = parsedPort
		}

		conn.SSH.Enabled = transport.Selected == "SSH Tunnel"
		if conn.SSH.Enabled {
			conn.SSH.Host = strings.TrimSpace(sshHost.Text)
			parsedSSHPort, err := parsePort(sshPort.Text, 22)
			if err != nil {
				return conn, fmt.Errorf("invalid ssh port: %w", err)
			}
			conn.SSH.Port = parsedSSHPort
			conn.SSH.User = strings.TrimSpace(sshUser.Text)
			if conn.SSH.Host == "" || conn.SSH.User == "" {
				return conn, fmt.Errorf("ssh host and user are required")
			}
			if sshAuth.Selected == "Key File" {
				conn.SSH.AuthType = model.SSHAuthKeyFile
				conn.SSH.KeyFile = strings.TrimSpace(sshKeyFile.Text)
				conn.SSH.Passphrase = sshPassphrase.Text
				if conn.SSH.KeyFile == "" {
					return conn, fmt.Errorf("ssh key file is required")
				}
			} else {
				conn.SSH.AuthType = model.SSHAuthPassword
				conn.SSH.Password = sshPassword.Text
				if conn.SSH.Password == "" {
					return conn, fmt.Errorf("ssh password is required")
				}
			}
		}

		return conn, nil
	}

	testButton := widget.NewButtonWithIcon("Test Connection", theme.ConfirmIcon(), func() {
		conn, err := buildConn()
		if err != nil {
			d.alertError(err)
			return
		}
		if err := d.dbService.TestConnection(conn); err != nil {
			d.alertError(err)
			return
		}
		dialog.ShowInformation("Connection Test", "Connection successful", d.win)
	})
	testButton.Importance = widget.LowImportance

	saveButton := widget.NewButtonWithIcon("Save", theme.DocumentSaveIcon(), func() {
		conn, err := buildConn()
		if err != nil {
			d.alertError(err)
			return
		}
		if _, err := d.repo.UpsertConnection(conn); err != nil {
			d.alertError(err)
			return
		}
		d.reloadPersistedData()
		d.refreshAllLists()
		d.syncConnectionHeader()
		formDialog.Hide()
	})
	saveButton.Importance = widget.HighImportance

	cancelButton := widget.NewButtonWithIcon("Cancel", theme.CancelIcon(), func() {
		if formDialog != nil {
			formDialog.Hide()
		}
	})

	commonPanel := d.panel(widget.NewForm(
		widget.NewFormItem("Connection Name", name),
		widget.NewFormItem("Database Type", typeRadio),
		widget.NewFormItem("Network", transport),
		widget.NewFormItem("Input Mode", modeSelect),
	))
	connectionPanel := d.panel(container.NewVBox(
		widget.NewLabelWithStyle("Connection Details", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabel("Use either a full connection string or manual host credentials."),
		connStringForm,
		manualForm,
	))
	content := container.NewVBox(
		commonPanel,
		d.vGap(8),
		connectionPanel,
		d.vGap(8),
		sshPanel,
	)
	scroll := container.NewVScroll(content)
	scroll.SetMinSize(fyne.NewSize(740, 560))
	footer := container.NewHBox(
		d.fixed(testButton, 138, 34),
		layout.NewSpacer(),
		d.fixed(saveButton, 118, 34),
		d.hGap(8),
		d.fixed(cancelButton, 118, 34),
	)
	header := container.NewVBox(
		widget.NewLabelWithStyle(title, fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabel("Configure direct or SSH tunnel access, then test before saving."),
		widget.NewSeparator(),
	)
	wrapper := container.NewBorder(header, footer, nil, nil, scroll)

	formDialog = dialog.NewCustomWithoutButtons(title, wrapper, d.win)
	formDialog.Show()
}

func (d *DesktopApp) deleteSelectedConnection() {
	conn, ok := d.currentSelectedConnection()
	if !ok {
		d.alertError(fmt.Errorf("select a connection first"))
		return
	}
	dialog.ShowConfirm("Delete Connection", "Delete selected connection?", func(confirm bool) {
		if !confirm {
			return
		}
		if err := d.repo.DeleteConnection(conn.ID); err != nil {
			d.alertError(err)
			return
		}
		d.dbService.Disconnect(conn.ID)
		if conn.ID == d.activeConnectionID {
			d.activeConnectionID = ""
			d.connStatus.SetText("Disconnected")
		}
		d.syncConnectionHeader()
		d.reloadPersistedData()
		d.refreshAllLists()
	}, d.win)
}

func (d *DesktopApp) reloadPersistedData() {
	d.connections = d.repo.Connections()
	d.snippets = d.repo.Snippets()
	d.history = d.repo.History()
}

func (d *DesktopApp) refreshAllLists() {
	d.syncConnectionHeader()
	if d.homeConnList != nil {
		d.refreshHomeConnections()
	}
	if d.connList != nil {
		for i := range d.connections {
			d.connList.SetItemHeight(i, 46)
		}
		d.connList.Refresh()
	}
	if d.tableList != nil {
		for i := range d.tableInfos {
			d.tableList.SetItemHeight(i, 42)
		}
		d.tableList.Refresh()
	}
	if d.snippetList != nil {
		for i := range d.snippets {
			d.snippetList.SetItemHeight(i, 54)
		}
		d.snippetList.Refresh()
	}
	if d.historyList != nil {
		for i := range d.history {
			d.historyList.SetItemHeight(i, 54)
		}
		d.historyList.Refresh()
	}
	if d.explorerTree != nil {
		d.explorerTree.Refresh()
	}
}

func (d *DesktopApp) currentSelectedConnection() (model.Connection, bool) {
	id := d.selectedConnection
	if id == "" {
		return model.Connection{}, false
	}
	for _, c := range d.connections {
		if c.ID == id {
			return c, true
		}
	}
	return model.Connection{}, false
}

func (d *DesktopApp) currentActiveConnection() (model.Connection, bool) {
	if d.activeConnectionID == "" {
		return model.Connection{}, false
	}
	for _, c := range d.connections {
		if c.ID == d.activeConnectionID {
			return c, true
		}
	}
	return model.Connection{}, false
}

func (d *DesktopApp) alertError(err error) {
	if err == nil {
		return
	}
	dialog.ShowError(err, d.win)
}

func (d *DesktopApp) updateSchemaOptions() {
	if d.schemaSelect == nil {
		return
	}

	set := map[string]struct{}{}
	for _, item := range d.objects {
		if item.Type == "schema" || item.Type == "database" {
			if strings.TrimSpace(item.Name) != "" {
				set[item.Name] = struct{}{}
			}
		}
		if strings.TrimSpace(item.Schema) != "" {
			set[item.Schema] = struct{}{}
		}
	}
	for _, item := range d.tableInfosAll {
		if strings.TrimSpace(item.Schema) != "" {
			set[item.Schema] = struct{}{}
		}
	}
	if strings.TrimSpace(d.selectedSchema) != "" {
		set[d.selectedSchema] = struct{}{}
	}

	options := make([]string, 0, len(set))
	for schema := range set {
		options = append(options, schema)
	}
	sort.Strings(options)

	d.syncingSchemaSelect = true
	d.schemaSelect.Options = options
	d.schemaSelect.Refresh()

	selected := d.selectedSchema
	if selected == "" && len(options) > 0 {
		selected = options[0]
		d.selectedSchema = selected
	}
	if selected != "" {
		d.schemaSelect.SetSelected(selected)
	}
	d.syncingSchemaSelect = false
}

func (d *DesktopApp) syncConnectionHeader() {
	conn, ok := d.currentActiveConnection()
	if !ok {
		if d.activeDB != nil {
			d.activeDB.SetText("No active database")
		}
		if d.activeSSH != nil {
			d.activeSSH.SetText("SSH Tunnel: inactive")
		}
		if d.queryDB != nil {
			d.queryDB.SetText("No active database")
		}
		return
	}

	if d.connStatus != nil {
		d.connStatus.SetText("Connected: " + conn.Name)
	}
	if d.activeDB != nil {
		d.activeDB.SetText(conn.Name)
	}
	if d.activeSSH != nil {
		if conn.SSH.Enabled {
			d.activeSSH.SetText("SSH Tunnel: active")
		} else {
			d.activeSSH.SetText("SSH Tunnel: direct connection")
		}
	}
	if d.queryDB != nil {
		d.queryDB.SetText(conn.Name + " (" + strings.ToUpper(string(conn.Type)) + ")")
	}
}

func (d *DesktopApp) backgroundRect() fyne.CanvasObject {
	bg := canvas.NewRectangle(d.pageColor())
	return bg
}

func (d *DesktopApp) panel(content fyne.CanvasObject) fyne.CanvasObject {
	bg := canvas.NewRectangle(d.panelColor())
	bg.StrokeColor = d.borderColor()
	bg.StrokeWidth = 1
	bg.CornerRadius = 12
	padded := d.pad(content, 8, 8, 10, 10)
	return container.NewStack(bg, padded)
}

func (d *DesktopApp) pad(content fyne.CanvasObject, top, bottom, left, right float32) fyne.CanvasObject {
	return container.New(layout.NewCustomPaddedLayout(top, bottom, left, right), content)
}

func (d *DesktopApp) vGap(size float32) fyne.CanvasObject {
	space := canvas.NewRectangle(color.Transparent)
	space.SetMinSize(fyne.NewSize(1, size))
	return space
}

func (d *DesktopApp) hGap(size float32) fyne.CanvasObject {
	space := canvas.NewRectangle(color.Transparent)
	space.SetMinSize(fyne.NewSize(size, 1))
	return space
}

func (d *DesktopApp) fixed(content fyne.CanvasObject, width, height float32) fyne.CanvasObject {
	return container.NewGridWrap(fyne.NewSize(width, height), content)
}

func (d *DesktopApp) pageColor() color.Color {
	if d.darkMode {
		return color.NRGBA{R: 6, G: 14, B: 30, A: 255}
	}
	return color.NRGBA{R: 238, G: 243, B: 252, A: 255}
}

func (d *DesktopApp) panelColor() color.Color {
	if d.darkMode {
		return color.NRGBA{R: 11, G: 24, B: 44, A: 255}
	}
	return color.NRGBA{R: 251, G: 253, B: 255, A: 255}
}

func (d *DesktopApp) borderColor() color.Color {
	if d.darkMode {
		return color.NRGBA{R: 27, G: 44, B: 71, A: 255}
	}
	return color.NRGBA{R: 210, G: 220, B: 237, A: 255}
}

func (d *DesktopApp) accentColor() color.Color {
	if d.darkMode {
		return color.NRGBA{R: 33, G: 102, B: 235, A: 255}
	}
	return color.NRGBA{R: 37, G: 99, B: 235, A: 255}
}

func labeledField(label string, field fyne.CanvasObject) fyne.CanvasObject {
	return container.NewVBox(
		widget.NewLabelWithStyle(strings.ToUpper(label), fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		field,
	)
}

func firstSchema(items []model.DBObject) string {
	for _, item := range items {
		if item.Type == "schema" || item.Type == "database" {
			return item.Name
		}
	}
	for _, item := range items {
		if item.Schema != "" {
			return item.Schema
		}
	}
	return ""
}

func tableKey(schema, table string) string {
	schema = strings.TrimSpace(schema)
	table = strings.TrimSpace(table)
	if schema == "" {
		return table
	}
	return schema + "." + table
}

func indexOfOpenTable(items []openTableEntry, target openTableEntry) int {
	for i, item := range items {
		if item.Schema == target.Schema && item.Name == target.Name {
			return i
		}
	}
	return -1
}

func indexOfOpenTableByKey(items []openTableEntry, key string) int {
	for i, item := range items {
		if tableKey(item.Schema, item.Name) == key {
			return i
		}
	}
	return -1
}

func formatInt(value int64) string {
	negative := value < 0
	if negative {
		value = -value
	}
	raw := strconv.FormatInt(value, 10)
	if len(raw) <= 3 {
		if negative {
			return "-" + raw
		}
		return raw
	}
	var b strings.Builder
	for i, r := range raw {
		if i > 0 && (len(raw)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	if negative {
		return "-" + b.String()
	}
	return b.String()
}

func compactText(value string, max int) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\n", " "))
	if len(value) <= max {
		return value
	}
	return value[:max] + "..."
}

func parsePort(text string, fallback int) (int, error) {
	if strings.TrimSpace(text) == "" {
		return fallback, nil
	}
	port, err := strconv.Atoi(strings.TrimSpace(text))
	if err != nil {
		return 0, err
	}
	return port, nil
}

func labelToType(label string) model.DBType {
	switch strings.ToLower(strings.TrimSpace(label)) {
	case "postgres":
		return model.DBTypePostgres
	case "mysql":
		return model.DBTypeMySQL
	case "mongo":
		return model.DBTypeMongo
	case "redis":
		return model.DBTypeRedis
	default:
		return model.DBTypePostgres
	}
}

func typeToLabel(dbType model.DBType) string {
	switch dbType {
	case model.DBTypePostgres:
		return "Postgres"
	case model.DBTypeMySQL:
		return "MySQL"
	case model.DBTypeMongo:
		return "Mongo"
	case model.DBTypeRedis:
		return "Redis"
	default:
		return "Postgres"
	}
}

func defaultPort(dbType model.DBType) int {
	switch dbType {
	case model.DBTypePostgres:
		return 5432
	case model.DBTypeMySQL:
		return 3306
	case model.DBTypeMongo:
		return 27017
	case model.DBTypeRedis:
		return 6379
	default:
		return 5432
	}
}

func indexOf(items []string, value string) int {
	for i, item := range items {
		if item == value {
			return i
		}
	}
	return -1
}

package public

import (
	"embed"
	"io/fs"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/config"
)

//go:embed frontend
var PublicFS embed.FS

// 常量定义
const (
	DataDir     = "./data"
	FaviconFile = "favicon.ico"
	DistDir     = "dist"       // 静态资源存放目录
	IndexFile   = "index.html" // 相对于 DistDir
)

// Static 注册静态资源和 SPA 路由处理
func Static(r *gin.RouterGroup, noRoute func(handlers ...gin.HandlerFunc)) {
	// 嵌入式前端静态资源，固定从 frontend 目录提供
	frontendFS, err := fs.Sub(PublicFS, "frontend")
	if err != nil {
		panic("you may forget to put dist of frontend to public/frontend/dist")
	}

	getConfig := func(c *gin.Context) map[string]any {
		cfg, _ := config.GetMany(map[string]any{
			config.DescriptionKey: "A simple server monitor tool.",
			config.CustomHeadKey:  "",
			config.CustomBodyKey:  "",
			config.SitenameKey:    "Komari Monitor",
		})
		return cfg
	}

	// 核心逻辑：获取文件内容
	// filePath: 相对于内置前端根目录的路径 (例如 "dist/assets/a.js")
	// 返回: content, contentType, exists
	getFileContent := func(relativePath string) ([]byte, string, bool) {
		cleanPath := strings.TrimPrefix(relativePath, "/")

		cleanPath = filepath.Clean(cleanPath)

		embedPath := filepath.ToSlash(cleanPath)

		if strings.Contains(embedPath, "..") {
			return nil, "", false
		}

		if content, err := fs.ReadFile(frontendFS, embedPath); err == nil {
			return content, mime.TypeByExtension(filepath.Ext(embedPath)), true
		}

		return nil, "", false
	}

	// 核心逻辑：渲染 Index.html
	serveIndex := func(c *gin.Context) {
		reqPath := c.Request.URL.Path
		cfg := getConfig(c)
		shouldReplace := true

		if strings.HasPrefix(reqPath, "/admin") || strings.HasPrefix(reqPath, "/terminal") {
			shouldReplace = false
		}

		targetFile := path.Join(DistDir, IndexFile)
		content, _, exists := getFileContent(targetFile)

		if !exists {
			c.String(http.StatusNotFound, "Index file missing (%s).", targetFile)
			return
		}

		// 如果不替换，直接返回原始内容
		if !shouldReplace {
			c.Data(http.StatusOK, "text/html; charset=utf-8", content)
			return
		}

		// 执行 HTML 内容替换
		htmlStr := string(content)
		replacer := strings.NewReplacer(
			"<title>Komari Monitor</title>", "<title>"+cfg[config.SitenameKey].(string)+"</title>",
			"A simple server monitor tool.", cfg[config.DescriptionKey].(string),
			"</head>", cfg[config.CustomHeadKey].(string)+"</head>",
			"</body>", cfg[config.CustomBodyKey].(string)+"</body>",
		)

		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(replacer.Replace(htmlStr)))
	}

	// ================= 路由定义 =================

	// 1. Favicon 优先策略
	r.GET("/favicon.ico", func(c *gin.Context) {
		// 优先：./data/favicon.ico
		localFavicon := filepath.Join(DataDir, FaviconFile)
		if _, err := os.Stat(localFavicon); err == nil {
			c.File(localFavicon)
			return
		}

		themeFaviconPath := path.Join(DistDir, FaviconFile)
		content, mimeType, exists := getFileContent(themeFaviconPath)
		if exists {
			c.Data(http.StatusOK, mimeType, content)
			return
		}

		c.Status(http.StatusNotFound)
	})

	// 2. 历史兼容入口：旧前端若仍引用 /themes/default/*，统一回落到内置静态资源。
	r.GET("/themes/:id/*path", func(c *gin.Context) {
		// c.Param("path") 包含了开头的 /，getFileContent 会处理
		filePath := c.Param("path")

		content, mimeType, exists := getFileContent(filePath)
		if exists {
			c.Data(http.StatusOK, mimeType, content)
			return
		}
		c.Status(http.StatusNotFound)
	})

	// 3. SPA 路由 (noRoute)
	noRoute(func(c *gin.Context) {
		if c.Request.Method != http.MethodGet {
			c.Status(http.StatusNotFound)
			return
		}
		reqPath := c.Request.URL.Path

		// SPA 静态资源回退
		distPath := path.Join(DistDir, reqPath)

		content, mimeType, exists := getFileContent(distPath)
		if exists {
			c.Data(http.StatusOK, mimeType, content)
			return
		}

		// 如果资源不存在，且路径包含扩展名 (如 .js, .css, .png)，则返回 404
		// 避免将 index.html 作为 js 文件返回导致 "Failed to fetch dynamically imported module"
		//ext := filepath.Ext(reqPath)
		//if ext != "" && ext != ".html" {
		//	c.Status(http.StatusNotFound)
		//	return
		//}

		// 路由 (如 /dashboard, /settings) -> 返回 index.html
		serveIndex(c)
	})
}

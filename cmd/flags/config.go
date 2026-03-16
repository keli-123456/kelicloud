package flags

var (
	// 数据库配置
	DatabaseType string // 仅用于测试环境切换 sqlite；运行时固定使用 mysql
	DatabaseFile string // 仅用于测试环境的 sqlite 数据库文件路径
	DatabaseHost string // MySQL 数据库主机地址
	DatabasePort string // MySQL 数据库端口
	DatabaseUser string // MySQL 数据库用户名
	DatabasePass string // MySQL 数据库密码
	DatabaseName string // MySQL 数据库名称

	Listen string
)

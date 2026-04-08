package records

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/komari-monitor/komari/database/models"
)

var uuid = "7901508c-304f-49aa-b84f-957c33ae6f8a"

var _ = func() bool {
	// 确保 Test 环境中使用 sqlite 内存数据库
	return true
}()

// TestCompactRecord tests the database compaction logic by inserting 12h30m of data (one record per minute),
// then running migrateOldRecords and verifying the aggregation and cleanup.
func TestCompactRecord(t *testing.T) {
	const totalMinutes = 12*60 + 30
	now := time.Date(2026, time.April, 8, 12, 30, 0, 0, time.UTC)
	threshold := now.Add(-4 * time.Hour)

	// 使用 sqlite 内存数据库并迁移表结构
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)
	assert.NoError(t, db.AutoMigrate(&models.Record{}))
	assert.NoError(t, db.Table("records_long_term").AutoMigrate(&models.Record{}))

	expectedGroups := make(map[time.Time]struct{})
	expectedRemain := 0

	// 插入数据
	for i := 0; i < totalMinutes; i++ {
		recTime := now.Add(-time.Duration(i) * time.Minute)
		rec := models.Record{Client: uuid, Time: models.FromTime(recTime), Cpu: float32(i), Gpu: float32(i), Load: float32(i), Temp: float32(i), Ram: int64(i)}
		err := db.Create(&rec).Error
		assert.NoError(t, err)

		if recTime.Before(threshold) {
			slot := recTime.Truncate(time.Hour)
			expectedGroups[slot] = struct{}{}
		} else {
			expectedRemain++
		}
	}

	// 运行压缩（迁移）逻辑
	err = migrateOldRecordsBefore(db, threshold)
	assert.NoError(t, err)

	// 验证 long-term 表中的聚合记录数
	var longCount int64
	assert.NoError(t, db.Table("records_long_term").Count(&longCount).Error)
	assert.Equal(t, int64(len(expectedGroups)), longCount)

	// 验证原始表中剩余记录数
	var remainCount int64
	assert.NoError(t, db.Table("records").Count(&remainCount).Error)
	assert.Equal(t, int64(expectedRemain), remainCount)

}

func TestCompactGPURecord(t *testing.T) {
	const totalMinutes = 12*60 + 30
	now := time.Date(2026, time.April, 8, 12, 30, 0, 0, time.UTC)
	threshold := now.Add(-4 * time.Hour)

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)
	assert.NoError(t, db.AutoMigrate(&models.GPURecord{}))
	assert.NoError(t, db.Table("gpu_records_long_term").AutoMigrate(&models.GPURecord{}))

	expectedGroups := make(map[time.Time]struct{})
	expectedRemain := 0

	for i := 0; i < totalMinutes; i++ {
		recTime := now.Add(-time.Duration(i) * time.Minute)
		rec := models.GPURecord{
			Client:      uuid,
			Time:        models.FromTime(recTime),
			DeviceIndex: 0,
			DeviceName:  "GPU 0",
			MemTotal:    int64(1000 + i),
			MemUsed:     int64(500 + i),
			Utilization: float32(i),
			Temperature: 40 + i%10,
		}
		err := db.Create(&rec).Error
		assert.NoError(t, err)

		if recTime.Before(threshold) {
			slot := recTime.Truncate(time.Hour)
			expectedGroups[slot] = struct{}{}
		} else {
			expectedRemain++
		}
	}

	err = migrateGPURecordsBefore(db, threshold)
	assert.NoError(t, err)

	var longCount int64
	assert.NoError(t, db.Table("gpu_records_long_term").Count(&longCount).Error)
	assert.Equal(t, int64(len(expectedGroups)), longCount)

	var remainCount int64
	assert.NoError(t, db.Table("gpu_records").Count(&remainCount).Error)
	assert.Equal(t, int64(expectedRemain), remainCount)
}
